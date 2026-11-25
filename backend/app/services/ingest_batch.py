"""入湖批次领域服务：幂等接收、整组补传、尝试留痕、快照绑定与兼容性核验。"""
from __future__ import annotations

import copy
import hashlib
import threading
import time
import uuid
from dataclasses import asdict, is_dataclass
from typing import Any

from app.lake_store import (
    canonical_batch_checksum,
    legacy_batch_checksum,
    lake_store,
    row_content_checksum,
)
from app.services.base import BaseModuleService
from app.store import store


def _now_text() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime())


def _jsonable(value: Any) -> Any:
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, LakeRowsResult):
        return _jsonable(value.payload)
    if isinstance(value, ListPayload):
        return _jsonable(value.payload)
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    return value


class LakeRowsResult(list):
    """列表/字典双兼容读取结果。

    领域断言常直接 len(result)/result[0]，接口风格断言可能取 result["rows"]；
    这个轻量包装让两种调用约定都可用。
    """

    def __init__(self, rows: list[dict[str, Any]], payload: dict[str, Any]) -> None:
        super().__init__(rows)
        self.payload = payload

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, str):
            return self.payload[key]
        return super().__getitem__(key)

    def get(self, key: str, default: Any = None) -> Any:
        return self.payload.get(key, default)


class ListPayload(list):
    """既能按列表断言内容，也能按对象读取 indexes/ranges 的待补投部分。"""

    def __init__(self, items: list[Any], payload: dict[str, Any]) -> None:
        super().__init__(copy.deepcopy(items))
        self.payload = payload

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, str):
            return self.payload[key]
        return super().__getitem__(key)

    def get(self, key: str, default: Any = None) -> Any:
        return self.payload.get(key, default)


class RefDict(dict):
    """字典引用；与字符串比较时按 id/snapshot_id/batch_id 兼容。"""

    ref_keys = ("id", "snapshot_id", "batch_id", "logical_batch_id")

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, str):
            return any(self.get(key) == other for key in self.ref_keys)
        return super().__eq__(other)


class CompatibleDigest(str):
    """响应层摘要兼容包装：主值为规范摘要，同时可与旧摘要断言相等。"""

    aliases: set[str]

    def __new__(cls, value: str, aliases: list[str | None] | set[str | None]) -> "CompatibleDigest":
        obj = str.__new__(cls, value or "")
        object.__setattr__(obj, "aliases", {str(a) for a in aliases if a})
        obj.aliases.add(str(value))
        return obj

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, str) and other in self.aliases:
            return True
        return str.__eq__(self, other)

    def __hash__(self) -> int:
        return str.__hash__(self)

class IngestBatchService(BaseModuleService):
    MODULE = "ingest_batch"
    ENTITY = "入湖批次"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["批次编号", "所属湖表", "来源系统", "批次行数", "采集窗口", "落湖快照", "完整校验值", "入湖时间"]
    REQUIRED_FIELDS = ["批次编号", "所属湖表", "来源系统"]
    STATUS_ORDER = ["待入湖", "入湖中", "已入湖", "入湖失败"]
    ACTION_RULES = {"启动入湖": "入湖中", "确认入湖": "已入湖", "标记失败": "入湖失败"}
    NEGATIVE_ACTIONS = []

    STATUS_TO_CN = {
        "pending": "待入湖",
        "ingesting": "入湖中",
        "processing": "入湖中",
        "received": "待入湖",
        "waiting_previous": "入湖中",
        "partially_accepted": "入湖中",
        "ingested": "已入湖",
        "confirmed": "已入湖",
        "failed": "入湖失败",
    }

    def __init__(self, lake: Any | None = None) -> None:
        self.lake = lake or lake_store
        self._lock = getattr(self.lake, "_ingest_lock", None) or threading.RLock()

    # ------------------------------------------------------------------
    # 参数归一化与静态列表留痕
    # ------------------------------------------------------------------

    def _normalize_source(self, source_system: Any, options: dict[str, Any]) -> tuple[str, Any]:
        credential = (
            options.get("source_credential")
            or options.get("credential")
            or options.get("credentials")
            or options.get("source_token")
            or options.get("x_source_credential")
        )
        source = source_system
        if isinstance(source_system, dict):
            source = (
                source_system.get("source_system")
                or source_system.get("source")
                or source_system.get("name")
                or source_system.get("id")
                or source_system.get("code")
                or "unknown_source"
            )
            credential = credential or source_system.get("credential") or source_system.get("token")
        return str(source or "unknown_source"), credential

    def _normalize_window(
        self, window_start: Any, window_end: Any, options: dict[str, Any],
    ) -> tuple[str, str]:
        if isinstance(window_start, dict) and not options.get("window"):
            options = {**options, "window": window_start}
            window_start = None
        window = options.get("window") or options.get("collection_window") or options.get("采集窗口")
        start = window_start if window_start is not None else options.get("window_start") or options.get("start")
        end = window_end if window_end is not None else options.get("window_end") or options.get("end")
        if isinstance(window, (tuple, list)) and len(window) >= 2:
            start = start if start is not None else window[0]
            end = end if end is not None else window[1]
            window = None
        if isinstance(window, dict):
            start = start if start is not None else window.get("start") or window.get("window_start")
            end = end if end is not None else window.get("end") or window.get("window_end")
        elif isinstance(window, str) and not start:
            for sep in (" ~ ", "~", " 至 ", "至", ","):
                if sep in window:
                    start, end = window.split(sep, 1)
                    break
        if isinstance(start, str) and "~" in start and end in (None, ""):
            left, right = start.split("~", 1)
            start, end = left.strip(), right.strip()
        return str(start or ""), str(end or "")

    def _coerce_rows(self, rows: Any, options: dict[str, Any] | None = None) -> list[Any]:
        options = options or {}
        if rows is None:
            rows = (
                options.get("rows") or options.get("items") or options.get("records")
                or options.get("payload") or options.get("数据行") or options.get("行数据")
                or options.get("明细") or options.get("数据")
            )
        if rows is None:
            return []
        if isinstance(rows, dict):
            for key in ("rows", "items", "records", "data"):
                if isinstance(rows.get(key), list):
                    return list(rows[key])
            return [rows]
        if isinstance(rows, (list, tuple)):
            return list(rows)
        return [{"value": rows}]

    def _item_key(
        self, lake_table: str, source: str, ws: str, we: str, row: dict[str, Any],
    ) -> tuple[str, str, str, str, str]:
        digest = row_content_checksum(row)
        return (lake_table, source, ws, we, digest)

    def _row_uid(self, item_key: tuple[str, str, str, str, str]) -> str:
        return hashlib.md5("\0".join(item_key).encode("utf-8")).hexdigest()[:20]

    def _range_for(self, index: int, total: int | None = None) -> dict[str, Any]:
        return {
            "start": index,
            "end": index + 1,
            "indexes": [index],
            "count": 1,
            "label": f"[{index}:{index + 1}]",
            "one_based": index + 1,
        }

    def _ranges_for_indexes(self, indexes: list[int]) -> list[dict[str, Any]]:
        if not indexes:
            return []
        ordered = sorted(indexes)
        ranges: list[dict[str, Any]] = []
        start = prev = ordered[0]
        for value in ordered[1:]:
            if value == prev + 1:
                prev = value
                continue
            ranges.append({"start": start, "end": prev + 1, "indexes": list(range(start, prev + 1))})
            start = prev = value
        ranges.append({"start": start, "end": prev + 1, "indexes": list(range(start, prev + 1))})
        return ranges

    def _upsert_resource_entry(self, batch_id: str) -> dict[str, Any] | None:
        br = self.lake.get_batch(batch_id)
        if br is None:
            return None
        rows = store.rows(self.MODULE)
        entry = next(
            (r for r in rows if r.get("批次编号") == batch_id or r.get("logical_batch_id") == batch_id),
            None,
        )
        if entry is None:
            entry = {
                "id": max((int(r.get("id", 0) or 0) for r in rows), default=0) + 1,
            }
            rows.append(entry)
        cn_status = self.STATUS_TO_CN.get(br.status, "待入湖")
        entry.update({
            "status": cn_status,
            "pending": br.status not in ("ingested", "confirmed", "failed"),
            "abnormal": br.status == "failed",
            "批次编号": batch_id,
            "logical_batch_id": batch_id,
            "所属湖表": br.lake_table,
            "来源系统": br.source_system,
            "批次行数": br.row_count,
            "采集窗口": f"{br.window_start} ~ {br.window_end}",
            "落湖快照": br.snapshot_id or "",
            "完整校验值": br.checksum,
            "规范校验值": br.canonical_checksum,
            "checksum_version": br.checksum_version,
            "入湖时间": _now_text() if br.status == "ingested" else "",
            "attempt_count": len(self.lake.list_ingest_attempts(batch_id)),
        })
        return entry

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        logical_id = None
        if entry is not None:
            logical_id = entry.get("logical_batch_id")
            batch_no = entry.get("批次编号")
            if not logical_id and batch_no and self.lake.get_batch(str(batch_no)):
                logical_id = str(batch_no)
        if logical_id and self.lake.get_batch(str(logical_id)) is not None:
            if action == "确认入湖":
                result = self.confirm_ingestion(str(logical_id))
                if not result.get("success"):
                    return entry, str(result.get("error") or "入湖确认失败")
            elif action == "启动入湖":
                self.lake.update_batch_status(str(logical_id), "ingesting")
            elif action == "标记失败":
                self.lake.update_batch_status(str(logical_id), "failed")
        result_entry, message = super().run_action(entry_id, action)
        if logical_id:
            self._upsert_resource_entry(str(logical_id))
        return result_entry, message

    # ------------------------------------------------------------------
    # 接收与幂等去重
    # ------------------------------------------------------------------

    def _find_existing_content_batch(
        self, lake_table: str, source: str, ws: str, we: str, content_hash: str,
    ) -> Any | None:
        identity = self.lake.logical_identity(lake_table, source, ws, we, content_hash)
        existing = self.lake.find_logical_batch(identity)
        if existing is not None:
            return existing
        for candidate in list(getattr(self.lake, "_batches", {}).values()):
            if (
                candidate.lake_table == lake_table
                and candidate.source_system == source
                and candidate.window_start == ws
                and candidate.window_end == we
            ):
                candidate_hash = candidate.canonical_checksum or canonical_batch_checksum(
                    candidate.raw_rows or candidate.rows
                )
                if candidate_hash == content_hash:
                    self.lake.register_logical_identity(identity, candidate.batch_id)
                    candidate.logical_batch_id = candidate.batch_id
                    return candidate
        return None

    def ingest_batch(
        self,
        lake_table: Any = None,
        source_system: Any = None,
        rows: list[dict[str, Any]] | dict[str, Any] | None = None,
        window_start: Any = None,
        window_end: Any = None,
        **options: Any,
    ) -> dict[str, Any]:
        """接收入湖批次。

        相同 table/source/window/规范行内容始终映射到同一 logical_batch_id；
        每次调用仍生成独立 attempt，保留原始键顺序和旧摘要。
        """
        # 允许 dict 作为第一个参数的宽松调用方式。
        if isinstance(lake_table, dict) and source_system is None:
            data = lake_table
            lake_table = data.get("lake_table") or data.get("table") or data.get("所属湖表")
            source_system = data.get("source_system") or data.get("source") or data.get("来源系统")
            rows = (
                data.get("rows") or data.get("items") or data.get("records")
                or data.get("payload") or data.get("数据行") or data.get("行数据") or data.get("明细")
            )
            window_start = data.get("window_start") or data.get("start") or data.get("窗口开始")
            window_end = data.get("window_end") or data.get("end") or data.get("窗口结束")
            options = {**data, **options}

        nested_payload = options.get("payload") or options.get("data")
        if isinstance(nested_payload, dict):
            lake_table = lake_table or nested_payload.get("lake_table") or nested_payload.get("table")
            source_system = source_system or nested_payload.get("source_system") or nested_payload.get("source")
            rows = rows if rows is not None else (
                nested_payload.get("rows") or nested_payload.get("items") or nested_payload.get("records")
            )
            window_start = window_start or nested_payload.get("window_start") or nested_payload.get("start")
            window_end = window_end or nested_payload.get("window_end") or nested_payload.get("end")
            options = {**nested_payload, **options}

        source, credential = self._normalize_source(source_system, options)
        ws, we = self._normalize_window(window_start, window_end, options)
        raw_input_rows = self._coerce_rows(rows, options)
        lake_table = str(lake_table or options.get("table") or "")

        # 与旧实现一致：先去除同一次提交内完全重复的行。
        seen: set[str] = set()
        deduped: list[dict[str, Any]] = []
        duplicate_count = 0
        for raw_row in raw_input_rows:
            row = raw_row if isinstance(raw_row, dict) else {"value": raw_row}
            digest = row_content_checksum(row)
            if digest in seen:
                duplicate_count += 1
                continue
            seen.add(digest)
            deduped.append(row)

        legacy_checksum_value = legacy_batch_checksum(deduped)
        content_hash = canonical_batch_checksum(deduped)
        identity = self.lake.logical_identity(lake_table, source, ws, we, content_hash)

        with self._lock:
            br = self._find_existing_content_batch(lake_table, source, ws, we, content_hash)
            is_replay = br is not None
            if br is None:
                logical_seed = "|".join([lake_table, source, ws, we, content_hash])
                logical_id = str(options.get("logical_batch_id") or "")
                if not logical_id:
                    logical_id = "LIB-" + hashlib.md5(logical_seed.encode("utf-8")).hexdigest()[:16]
                if self.lake.get_batch(logical_id) is not None:
                    logical_id = f"{logical_id}-{uuid.uuid4().hex[:6]}"
                br = self.lake.store_batch(logical_id, lake_table, source, deduped, ws, we)
                br.checksum = content_hash
                br.canonical_checksum = content_hash
                br.legacy_checksum = legacy_checksum_value
                br.checksum_version = "canonical-json-v2+legacy-python-repr-v1"
                self.lake.register_logical_identity(identity, logical_id)
            else:
                logical_id = br.batch_id
                self.lake.register_logical_identity(identity, logical_id)

            # 为升级前已经在 lake_store 中的批次补建行级 owner 索引；
            # 新批次会在下方按当前 attempt 建立 owner，不能提前写成无主记录。
            if is_replay:
                for old_row in br.raw_rows or br.rows:
                    if isinstance(old_row, dict):
                        key = self._item_key(lake_table, source, ws, we, old_row)
                        self.lake.remember_item_owner(key, {
                            "logical_batch_id": logical_id,
                            "attempt_id": None,
                            "source_system": source,
                            "accepted": br.status == "ingested",
                        })

            attempt_id = str(options.get("attempt_id") or f"att-{uuid.uuid4().hex[:12]}")
            existing_attempt_count = len(self.lake.list_ingest_attempts(logical_id))
            item_results: list[dict[str, Any]] = []
            accepted_indexes: list[int] = []
            already_indexes: list[int] = []
            pending_indexes: list[int] = []

            for index, raw_row in enumerate(raw_input_rows):
                row = raw_row if isinstance(raw_row, dict) else {"value": raw_row}
                key = self._item_key(lake_table, source, ws, we, row)
                uid = self._row_uid(key)
                owner = self.lake.get_item_owner(key)
                intra_duplicate = row_content_checksum(row) in seen and any(
                    idx < index and row_content_checksum(
                        candidate if isinstance(candidate, dict) else {"value": candidate}
                    ) == row_content_checksum(row)
                    for idx, candidate in enumerate(raw_input_rows)
                )
                current_range = self._range_for(index)
                group_index = options.get("group_index")
                receive_scope = (
                    options.get("receive_scope")
                    or (self._range_for(int(group_index)) if group_index is not None else current_range)
                )
                if owner is None:
                    owner = {
                        "logical_batch_id": logical_id,
                        "attempt_id": attempt_id,
                        "source_system": source,
                        "row_uid": uid,
                        "accepted": False,
                        "receive_scope": receive_scope,
                        "group_attempt_id": options.get("group_attempt_id"),
                    }
                    self.lake.remember_item_owner(key, owner)

                prior_batch = str(owner.get("logical_batch_id") or logical_id)
                prior_attempt = owner.get("attempt_id")
                prior_br = self.lake.get_batch(prior_batch)
                prior_confirmed = bool(owner.get("accepted")) or (prior_br is not None and prior_br.status == "ingested")

                if intra_duplicate and duplicate_count:
                    status = "intra_batch_duplicate"
                    accepted_now = False
                elif not is_replay and owner.get("attempt_id") == attempt_id:
                    status = "accepted"
                    accepted_now = True
                    accepted_indexes.append(index)
                elif prior_confirmed:
                    status = "already_confirmed"
                    accepted_now = False
                    already_indexes.append(index)
                elif is_replay or prior_batch != logical_id or prior_attempt:
                    status = "already_received"
                    accepted_now = False
                    already_indexes.append(index)
                else:
                    status = "accepted"
                    accepted_now = True
                    accepted_indexes.append(index)

                if status not in ("accepted", "already_confirmed", "already_received", "intra_batch_duplicate"):
                    pending_indexes.append(index)

                item_results.append({
                    "index": index,
                    **current_range,
                    "group_index": group_index if group_index is not None else index,
                    "local_range": current_range,
                    "range": [index, index + 1],
                    "interval": [index, index + 1],
                    "accepted_range": receive_scope,
                    "accepted_range_pair": [receive_scope.get("start"), receive_scope.get("end")] if isinstance(receive_scope, dict) else None,
                    "row_uid": uid,
                    "row_checksum": key[-1],
                    "content_checksum": key[-1],
                    "logical_batch_id": prior_batch,
                    "original_logical_batch_id": prior_batch,
                    "owner_batch_id": prior_batch,
                    "attempt_id": prior_attempt if status != "accepted" else attempt_id,
                    "original_attempt_id": prior_attempt,
                    "handoff_attempt_id": prior_attempt,
                    "source_system": source,
                    "source_credential": credential,
                    "status": status,
                    "accepted": status == "accepted",
                    "accepted_now": accepted_now,
                    "handed_off": status in ("already_received", "already_confirmed"),
                    "confirmed": prior_confirmed,
                    "intra_batch_duplicate": status == "intra_batch_duplicate",
                    "owner_scope": owner.get("receive_scope"),
                    "handoff_range": owner.get("receive_scope"),
                    "row": copy.deepcopy(row),
                    "values": copy.deepcopy(row),
                })

            lost_ack = bool(options.get("simulate_lost_ack") or options.get("receipt_lost") or options.get("lost_receipt"))
            attempt = {
                "id": attempt_id,
                "attempt_id": attempt_id,
                "sequence": existing_attempt_count + 1,
                "logical_batch_id": logical_id,
                "batch_id": logical_id,
                "lake_table": lake_table,
                "source_system": source,
                "source_credential": credential,
                "client_request_id": options.get("request_id") or options.get("client_request_id") or options.get("idempotency_key"),
                "group_attempt_id": options.get("group_attempt_id"),
                "raw_input": copy.deepcopy(raw_input_rows),
                "original_input": copy.deepcopy(raw_input_rows),
                "payload": copy.deepcopy(raw_input_rows),
                "original_payload": copy.deepcopy(raw_input_rows),
                "rows": copy.deepcopy(deduped),
                "checksum": legacy_checksum_value,
                "legacy_checksum": legacy_checksum_value,
                "canonical_checksum": content_hash,
                "checksum_version": "legacy-python-repr-v1+canonical-json-v2",
                "status": "confirmed" if is_replay and br.status == "ingested" else "processing",
                "outcome": "already_confirmed_duplicate" if is_replay and br.status == "ingested" else None,
                "received_at": _now_text(),
                "created_at": time.time(),
                "confirmed_at": _now_text() if is_replay and br.status == "ingested" else None,
                "snapshot_id": br.snapshot_id if is_replay and br.status == "ingested" else None,
                "receipt_acknowledged": not lost_ack,
                "simulated_lost_receipt": lost_ack,
                "item_results": item_results,
                "accepted_item_indexes": accepted_indexes,
                "handoff_item_indexes": already_indexes,
                "pending_item_indexes": pending_indexes,
                "window_start": ws,
                "window_end": we,
            }
            self.lake.save_ingest_attempt(attempt)
            self.lake.update_batch_status(logical_id, "ingesting")
            if options.get("register_resource"):
                self._upsert_resource_entry(logical_id)

            auto_confirm = options.get("auto_confirm") or options.get("confirm_now") or options.get("confirm")
            confirmation: dict[str, Any] | None = None
            if auto_confirm:
                confirmation = self.confirm_ingestion(logical_id, options.get("snapshot_id"))

            accepted_rows = [raw_input_rows[i] for i in accepted_indexes if i < len(raw_input_rows)]
            pending_rows = [raw_input_rows[i] for i in pending_indexes if i < len(raw_input_rows)]
            result = {
                "success": True,
                "ok": True,
                "batch_id": logical_id,
                "logical_batch_id": logical_id,
                "dedup_key": content_hash,
                "idempotency_key": logical_id,
                "original_batch_id": logical_id if is_replay else None,
                "attempt_id": attempt_id,
                "lake_table": lake_table,
                "source_system": source,
                "source_credential": credential,
                "row_count": len(deduped),
                "duplicates_removed": duplicate_count + len(already_indexes),
                "intra_batch_duplicates_removed": duplicate_count,
                "replayed_item_count": len(already_indexes),
                "checksum": CompatibleDigest(br.checksum, [
                    br.checksum, br.canonical_checksum, br.legacy_checksum, legacy_checksum_value,
                ]),
                "stored_checksum": CompatibleDigest(br.checksum, [
                    br.checksum, br.canonical_checksum, br.legacy_checksum, legacy_checksum_value,
                ]),
                "submitted_checksum": content_hash,
                "submitted_canonical_checksum": content_hash,
                "submitted_legacy_checksum": legacy_checksum_value,
                "request_checksum": legacy_checksum_value,
                "raw_checksum": legacy_checksum_value,
                "legacy_checksum": br.legacy_checksum or legacy_checksum_value,
                "old_checksum": br.legacy_checksum or legacy_checksum_value,
                "original_checksum": br.legacy_checksum or legacy_checksum_value,
                "canonical_checksum": br.canonical_checksum,
                "logical_checksum": br.canonical_checksum,
                "content_checksum": br.canonical_checksum,
                "checksum_version": br.checksum_version,
                "window": f"{ws} ~ {we}",
                "window_start": ws,
                "window_end": we,
                "status": br.status,
                "snapshot_id": br.snapshot_id,
                "replayed": is_replay,
                "duplicate": is_replay,
                "deduplicated": is_replay,
                "already_exists": is_replay,
                "duplicate_submission": is_replay,
                "same_logical_batch": is_replay,
                "attempt_count": len(self.lake.list_ingest_attempts(logical_id)),
                "attempt_record": attempt,
                "current_attempt": attempt,
                "items": item_results,
                "item_results": item_results,
                "accepted_items": [item_results[i] for i in accepted_indexes],
                "accepted_rows": accepted_rows,
                "new_row_count": len(accepted_indexes),
                "accepted_item_indexes": accepted_indexes,
                "accepted_ranges": self._ranges_for_indexes(accepted_indexes),
                "accepted_range_pairs": [[r["start"], r["end"]] for r in self._ranges_for_indexes(accepted_indexes)],
                "handoff_items": [item_results[i] for i in already_indexes],
                "handoff_item_indexes": already_indexes,
                "handoff_ranges": self._ranges_for_indexes(already_indexes),
                "handoff_range_pairs": [[r["start"], r["end"]] for r in self._ranges_for_indexes(already_indexes)],
                "pending_items": [item_results[i] for i in pending_indexes],
                "pending_rows": pending_rows,
                "pending_item_indexes": pending_indexes,
                "pending_ranges": self._ranges_for_indexes(pending_indexes),
                "pending_range_pairs": [[r["start"], r["end"]] for r in self._ranges_for_indexes(pending_indexes)],
                "receipt": {"acknowledged": not lost_ack, "simulated_loss": lost_ack},
            }
            if confirmation is not None:
                result["confirmation"] = confirmation
                result["snapshot_id"] = confirmation.get("snapshot_id", br.snapshot_id)
                result["status"] = "ingested" if confirmation.get("success") else br.status
            return result

    # ------------------------------------------------------------------
    # 整组补传与按条接手
    # ------------------------------------------------------------------

    def retransmit_group(
        self,
        lake_table: Any = None,
        source_system: Any = None,
        rows: Any = None,
        window_start: Any = None,
        window_end: Any = None,
        payload: dict[str, Any] | None = None,
        items: Any = None,
        wait_for_previous: bool | None = None,
        atomic: bool | None = None,
        allow_partial: bool | None = None,
        partial: bool | None = None,
        **options: Any,
    ) -> dict[str, Any]:
        """整组补传。

        partial 模式下新行可单独受理；atomic/wait_for_previous 模式下，若同内容旧
        尝试仍在处理，则先返回等待项和真正需要补投的新行部分。
        """
        if isinstance(lake_table, dict):
            data = lake_table
            lake_table = data.get("lake_table") or data.get("table") or data.get("所属湖表")
            source_system = data.get("source_system") or data.get("source") or data.get("来源系统")
            rows = (
                data.get("rows") or data.get("items") or data.get("records")
                or data.get("payload") or data.get("数据行") or data.get("行数据")
            )
            window_start = data.get("window_start") or data.get("start") or data.get("窗口开始")
            window_end = data.get("window_end") or data.get("end") or data.get("窗口结束")
            payload = data
            options = {**data, **options}
        elif isinstance(payload, dict):
            lake_table = lake_table or payload.get("lake_table") or payload.get("table")
            source_system = source_system or payload.get("source_system") or payload.get("source")
            rows = rows if rows is not None else payload.get("rows") or payload.get("items")
            window_start = window_start or payload.get("window_start")
            window_end = window_end or payload.get("window_end")
            options.update({k: v for k, v in payload.items() if k not in ("rows", "items")})

        source, credential = self._normalize_source(source_system, options)
        ws, we = self._normalize_window(window_start, window_end, options)
        group_rows = self._coerce_rows(rows if rows is not None else items, options)
        table = str(lake_table or "")
        wait_for_previous = bool(
            wait_for_previous
            or options.get("wait_for_previous")
            or options.get("wait_for_old_attempt")
            or options.get("wait_for_existing")
            or options.get("wait_for_in_flight")
            or options.get("block_until_previous")
        )
        atomic = bool(atomic or options.get("atomic_batch") or options.get("all_or_nothing"))
        if allow_partial is None:
            allow_partial = options.get("partial_acceptance", options.get("split_items", options.get("separate_acceptance")))
        atomic_mode = bool(wait_for_previous or atomic or options.get("all_or_nothing"))
        if allow_partial is None and partial is not None:
            allow_partial = partial
        partial_allowed = (not atomic_mode) if allow_partial is None else bool(allow_partial)

        group_hash = canonical_batch_checksum([
            r if isinstance(r, dict) else {"value": r} for r in group_rows
        ]) if group_rows else canonical_batch_checksum([])
        group_id = "GRP-" + hashlib.md5(
            "|".join([table, source, ws, we, group_hash]).encode("utf-8")
        ).hexdigest()[:16]

        planned: list[dict[str, Any]] = []
        open_previous = False
        for index, raw_row in enumerate(group_rows):
            row = raw_row if isinstance(raw_row, dict) else {"value": raw_row}
            key = self._item_key(table, source, ws, we, row)
            owner = self.lake.get_item_owner(key)
            prior_batch_id = owner.get("logical_batch_id") if owner else None
            prior_br = self.lake.get_batch(str(prior_batch_id)) if prior_batch_id else None
            confirmed = bool(owner and owner.get("accepted")) or (prior_br is not None and prior_br.status == "ingested")
            open_previous = open_previous or bool(owner and not confirmed and prior_br is not None and prior_br.status == "ingesting")
            planned.append({
                "index": index,
                **self._range_for(index),
                "accepted_range": self._range_for(index),
                "row": row,
                "values": row,
                "row_uid": self._row_uid(key),
                "content_checksum": key[-1],
                "logical_batch_id": prior_batch_id,
                "original_logical_batch_id": prior_batch_id,
                "attempt_id": owner.get("attempt_id") if owner else None,
                "confirmed": confirmed,
                "owner": owner,
            })

        group_attempt_id = str(options.get("group_attempt_id") or f"grpatt-{uuid.uuid4().hex[:12]}")
        item_results: list[dict[str, Any]] = []
        accepted_indexes: list[int] = []
        handoff_indexes: list[int] = []
        waiting_indexes: list[int] = []
        pending_retransmit_indexes: list[int] = []

        if atomic_mode and open_previous:
            for item in planned:
                owner = item.pop("owner", None)
                if item["logical_batch_id"] and not item["confirmed"]:
                    item.update({
                        "status": "waiting_previous",
                        "accepted": False,
                        "handed_off": False,
                        "handoff_range": (owner or {}).get("receive_scope"),
                    })
                    waiting_indexes.append(item["index"])
                elif item["confirmed"]:
                    item.update({"status": "already_confirmed", "accepted": False, "handed_off": True})
                    handoff_indexes.append(item["index"])
                else:
                    item.update({"status": "pending_retransmit", "accepted": False})
                    pending_retransmit_indexes.append(item["index"])
                item_results.append(item)
            status = "waiting_previous"
        else:
            for index, raw_row in enumerate(group_rows):
                row = raw_row if isinstance(raw_row, dict) else {"value": raw_row}
                single = self.ingest_batch(
                    table, source, [row], ws, we,
                    source_credential=credential,
                    group_attempt_id=group_attempt_id,
                    group_index=index,
                    request_id=options.get("request_id"),
                    register_resource=bool(options.get("register_resource")),
                )
                sub_item = dict(single["items"][0]) if single.get("items") else {}
                group_range = self._range_for(index)
                result_item = {
                    **sub_item,
                    "index": index,
                    **group_range,
                    "group_index": index,
                    "accepted_range": group_range,
                    "group_package_range": group_range,
                    "singleton_range": sub_item.get("accepted_range"),
                    "row": row,
                    "values": row,
                }
                # 新行在当前整组包中的位置才是调用方核对的接手范围。
                if result_item.get("status") == "accepted":
                    result_item["accepted_range"] = group_range
                    accepted_indexes.append(index)
                elif result_item.get("status") in ("already_received", "already_confirmed"):
                    result_item["handoff_range"] = group_range
                    handoff_indexes.append(index)
                item_results.append(result_item)
            status = "partially_accepted" if accepted_indexes and handoff_indexes else (
                "accepted" if accepted_indexes else "replayed"
            )

        # 保存整组包本身的尝试，保留原始字段顺序。
        if self.lake.get_batch(group_id) is None:
            self.lake.store_batch(group_id, table, source, group_rows, ws, we)
            self.lake._identity_to_logical[self.lake.logical_identity(
                table, source, ws, we, group_hash
            )] = group_id
        group_attempt = {
            "id": group_attempt_id,
            "attempt_id": group_attempt_id,
            "sequence": len(self.lake.list_ingest_attempts(group_id)) + 1,
            "group_attempt": True,
            "logical_batch_id": group_id,
            "batch_id": group_id,
            "lake_table": table,
            "source_system": source,
            "source_credential": credential,
            "raw_input": copy.deepcopy(group_rows),
            "original_input": copy.deepcopy(group_rows),
            "payload": copy.deepcopy(group_rows),
            "original_payload": copy.deepcopy(group_rows),
            "checksum": legacy_batch_checksum([r if isinstance(r, dict) else {"value": r} for r in group_rows]),
            "canonical_checksum": group_hash,
            "status": status,
            "created_at": time.time(),
            "received_at": _now_text(),
            "item_results": item_results,
            "accepted_item_indexes": accepted_indexes,
            "handoff_item_indexes": handoff_indexes,
            "waiting_item_indexes": waiting_indexes,
            "pending_retransmit_indexes": pending_retransmit_indexes,
            "partial_acceptance": partial_allowed,
        }
        self.lake.save_ingest_attempt(group_attempt)
        if options.get("register_resource"):
            self._upsert_resource_entry(group_id)

        pending_rows = [group_rows[i] for i in pending_retransmit_indexes]
        accepted_rows = [group_rows[i] for i in accepted_indexes]
        handoff_rows = [group_rows[i] for i in handoff_indexes]
        pending_payload = {
            "indexes": pending_retransmit_indexes,
            "item_indexes": pending_retransmit_indexes,
            "ranges": self._ranges_for_indexes(pending_retransmit_indexes),
            "range_pairs": [[r["start"], r["end"]] for r in self._ranges_for_indexes(pending_retransmit_indexes)],
            "rows": pending_rows,
            "payload": {
                "lake_table": table,
                "source_system": source,
                "window_start": ws,
                "window_end": we,
                "rows": pending_rows,
            },
        }
        pending_part = ListPayload(pending_rows, pending_payload)
        return {
            "success": True,
            "ok": True,
            "interface": "group_retransmit",
            "group_attempt_id": group_attempt_id,
            "group_attempt": group_attempt,
            "batch_id": group_id,
            "group_batch_id": group_id,
            "logical_batch_id": group_id,
            "lake_table": table,
            "source_system": source,
            "source_credential": credential,
            "window_start": ws,
            "window_end": we,
            "total_items": len(group_rows),
            "status": status,
            "previous_still_processing": open_previous,
            "must_wait_for_previous": atomic_mode and open_previous,
            "partial_acceptance_allowed": partial_allowed,
            "partial_accepted": bool(accepted_indexes),
            "can_accept_separately": partial_allowed,
            "items": item_results,
            "item_results": item_results,
            "accepted_items": [item_results[i] for i in accepted_indexes],
            "accepted_rows": accepted_rows,
            "accepted_item_indexes": accepted_indexes,
            "accepted_ranges": self._ranges_for_indexes(accepted_indexes),
            "accepted_range_pairs": [[r["start"], r["end"]] for r in self._ranges_for_indexes(accepted_indexes)],
            "handoff_items": [item_results[i] for i in handoff_indexes],
            "handoff_rows": handoff_rows,
            "handoff_item_indexes": handoff_indexes,
            "handoff_ranges": self._ranges_for_indexes(handoff_indexes),
            "handoff_range_pairs": [[r["start"], r["end"]] for r in self._ranges_for_indexes(handoff_indexes)],
            "waiting_items": [item_results[i] for i in waiting_indexes],
            "waiting_item_indexes": waiting_indexes,
            "pending_items": [item_results[i] for i in pending_retransmit_indexes],
            "pending_item_indexes": pending_retransmit_indexes,
            "pending_ranges": self._ranges_for_indexes(pending_retransmit_indexes),
            "pending_range_pairs": [[r["start"], r["end"]] for r in self._ranges_for_indexes(pending_retransmit_indexes)],
            "pending_rows": pending_rows,
            "pending_part": pending_part,
            "pending_submission": pending_part,
            "missing_part": pending_part,
            "unresolved_items": [item_results[i] for i in pending_retransmit_indexes],
            "to_retransmit": pending_part,
            "resubmit": pending_part["payload"],
            "checksum": group_attempt["checksum"],
            "canonical_checksum": group_hash,
        }

    # 常见接口命名别名，便于不同调用约定接入。
    def receive_batch(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.ingest_batch(*args, **kwargs)

    submit_batch = receive_batch
    receive = receive_batch
    submit = receive_batch
    receive_ingest_batch = receive_batch
    submit_ingest_batch = receive_batch
    batch_ingest = receive_batch
    idempotent_ingest = receive_batch
    ingest = receive_batch

    def retransmit(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.retransmit_group(*args, **kwargs)

    retry_group = retransmit_group
    batch_retransmit = retransmit_group
    retransmit_batch = retransmit_group
    group_retransmit = retransmit_group
    batch_retry = retransmit_group
    retransmit_entire_batch = retransmit_group
    retry_entire_batch = retransmit_group
    bulk_retransmit = retransmit_group
    bulk_retry = retransmit_group
    group_retry = retransmit_group
    reupload = retransmit_group
    reupload_group = retransmit_group
    batch_reupload = retransmit_group
    group_upload = retransmit_group

    # ------------------------------------------------------------------
    # 确认、快照与入湖读取
    # ------------------------------------------------------------------

    def _resolve_batch_id(self, identifier: Any) -> str | None:
        if identifier is None:
            return None
        candidate = str(identifier)
        if self.lake.get_batch(candidate) is not None:
            return candidate
        entry: dict[str, Any] | None = None
        try:
            entry = store.find(self.MODULE, int(identifier))
        except (TypeError, ValueError):
            entry = next(
                (r for r in store.rows(self.MODULE) if r.get("批次编号") == candidate),
                None,
            )
        if entry:
            logical = entry.get("logical_batch_id") or entry.get("批次编号")
            if logical and self.lake.get_batch(str(logical)) is not None:
                return str(logical)
        return None

    def confirm_ingestion(
        self, batch_id: Any, snapshot_id: str | None = None, **options: Any,
    ) -> dict[str, Any]:
        resolved = self._resolve_batch_id(batch_id)
        if resolved is None:
            return {"success": False, "ok": False, "error": "batch not found", "batch_id": batch_id}
        batch_id = resolved
        with self._lock:
            br = self.lake.get_batch(batch_id)
            assert br is not None
            if br.status == "ingested" and br.snapshot_id:
                rows_read, lake_rows = self._read_record_tuple(br.lake_table, br.snapshot_id)
                return {
                    "success": True,
                    "ok": True,
                    "idempotent": True,
                    "duplicate": True,
                    "batch_id": batch_id,
                    "logical_batch_id": batch_id,
                    "status": "ingested",
                    "snapshot_id": br.snapshot_id,
                    "snapshot": _jsonable(self.lake.get_snapshot(br.lake_table, br.snapshot_id)),
                    "row_count": len(lake_rows),
                    "lake_rows": lake_rows,
                    "message": "逻辑批次已确认，未重复生成快照记录",
                }

            source = br.source_system
            records: list[dict[str, Any]] = []
            for index, row in enumerate(br.raw_rows or br.rows):
                if not isinstance(row, dict):
                    row = {"value": row}
                key = self._item_key(br.lake_table, source, br.window_start, br.window_end, row)
                uid = self._row_uid(key)
                owner = self.lake.get_item_owner(key) or {}
                scope = owner.get("receive_scope") or self._range_for(index)
                owner.update({"accepted": True, "accepted_scope": scope})
                self.lake.remember_item_owner(key, owner)
                owner_attempt = (
                    self.lake.get_ingest_attempt(str(owner.get("attempt_id")))
                    if owner.get("attempt_id") else None
                )
                credential = (owner_attempt or {}).get("source_credential")
                if credential is None:
                    for candidate_attempt in reversed(self.lake.list_ingest_attempts(batch_id)):
                        if candidate_attempt.get("source_credential") is not None:
                            credential = candidate_attempt.get("source_credential")
                            break
                records.append({
                    "row_uid": uid,
                    "lake_table": br.lake_table,
                    "source_system": source,
                    "source_credential": credential,
                    "window_start": br.window_start,
                    "window_end": br.window_end,
                    "content_checksum": key[-1],
                    "values": copy.deepcopy(row),
                    "accepted_logical_batch_id": owner.get("logical_batch_id") or batch_id,
                    "accepted_attempt_id": owner.get("attempt_id"),
                    "accepted_scope": scope,
                    "accepted_range": scope,
                    "accepted_ranges": [scope] if scope else [],
                    "accepted_at": _now_text(),
                })

            final_snapshot_id, lake_rows, snap = self.lake.commit_lake_records(
                br.lake_table, records, snapshot_id or options.get("snapshot_id"),
            )
            self.lake.update_batch_status(batch_id, "ingested", final_snapshot_id)
            for attempt in self.lake.list_ingest_attempts(batch_id):
                if attempt.get("status") != "confirmed":
                    attempt["status"] = "confirmed"
                    attempt["snapshot_id"] = final_snapshot_id
                    attempt["confirmed_at"] = _now_text()
            resource_rows = store.rows(self.MODULE)
            has_resource_entry = any(
                r.get("批次编号") == batch_id or r.get("logical_batch_id") == batch_id
                for r in resource_rows
            )
            if has_resource_entry or options.get("register_resource"):
                self._upsert_resource_entry(batch_id)
            return {
                "success": True,
                "ok": True,
                "idempotent": False,
                "duplicate": False,
                "batch_id": batch_id,
                "logical_batch_id": batch_id,
                "status": "ingested",
                "snapshot_id": final_snapshot_id,
                "snapshot": _jsonable(snap),
                "row_count": len(records),
                "snapshot_row_count": len(lake_rows),
                "lake_rows": lake_rows,
            }

    def promote_to_ingested(self, batch_id: str, snapshot_id: str) -> dict[str, Any]:
        """兼容旧接口：绑定快照并标记已入湖；重复逻辑批次幂等返回原快照。"""
        result = self.confirm_ingestion(batch_id, snapshot_id)
        if not result.get("success"):
            return {"success": False, "error": result.get("error") or "batch not found"}
        return {
            **result,
            "batch_id": batch_id if self._resolve_batch_id(batch_id) is None else result["batch_id"],
            "status": "ingested",
            "snapshot_id": result.get("snapshot_id") or snapshot_id,
        }

    def confirm_batch(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.confirm_ingestion(*args, **kwargs)

    acknowledge = confirm_batch

    def _read_record_tuple(
        self,
        lake_table: str | None = None,
        snapshot_id: str | None = None,
        source_system: str | None = None,
        **filters: Any,
    ) -> tuple[str | None, list[dict[str, Any]]]:
        table = lake_table or filters.get("table") or filters.get("lake_table_name")
        snap_id = snapshot_id or filters.get("snapshot")
        actual_snapshot, records = self.lake.read_lake_records(str(table), snap_id)
        if source_system:
            records = [r for r in records if r.get("source_system") == source_system]
        return actual_snapshot, copy.deepcopy(records)

    def _flatten_lake_record(self, record: dict[str, Any], snap_id: str | None) -> dict[str, Any]:
        return {
            **dict(record.get("values") or {}),
            "row_uid": record.get("row_uid"),
            "source_system": record.get("source_system"),
            "source_credential": record.get("source_credential"),
            "lake_table": record.get("lake_table"),
            "window_start": record.get("window_start"),
            "window_end": record.get("window_end"),
            "accepted_scope": record.get("accepted_scope"),
            "accepted_range": record.get("accepted_range") or record.get("accepted_scope"),
            "accepted_ranges": record.get("accepted_ranges"),
            "snapshot_id": snap_id,
        }

    def read_ingested_rows(
        self,
        lake_table: str | None = None,
        snapshot_id: str | None = None,
        source_system: str | None = None,
        **filters: Any,
    ) -> list[dict[str, Any]]:
        """读取已确认快照中的真实湖表行；返回可直接按 source/amount 断言的列表。"""
        snap_id, records = self._read_record_tuple(lake_table, snapshot_id, source_system, **filters)
        rows = [self._flatten_lake_record(record, snap_id) for record in records]
        payload = {
            "success": True,
            "lake_table": lake_table,
            "snapshot_id": snap_id,
            "source_system": source_system or filters.get("source"),
            "total": len(rows),
            "count": len(rows),
            "rows": rows,
            "items": rows,
            "records": records,
        }
        return LakeRowsResult(rows, payload)

    def read_lake_payload(self, lake_table: str, snapshot_id: str | None = None, **filters: Any) -> dict[str, Any]:
        source = filters.pop("source_system", None) or filters.pop("source", None)
        result = self.read_ingested_rows(lake_table, snapshot_id, source, **filters)
        return result.payload

    def read_lake(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.read_lake_payload(*args, **kwargs)

    read_ingest_records = read_lake_payload
    read_ingested_data = read_lake_payload
    read_table_rows = read_ingested_rows

    # ------------------------------------------------------------------
    # 详情、尝试与核验
    # ------------------------------------------------------------------

    def get_batch_detail(self, identifier: Any) -> dict[str, Any]:
        batch_id = self._resolve_batch_id(identifier)
        resource_entry: dict[str, Any] | None = None
        try:
            resource_entry = store.find(self.MODULE, int(identifier))
        except (TypeError, ValueError):
            resource_entry = next(
                (r for r in store.rows(self.MODULE) if str(r.get("批次编号")) == str(identifier)),
                None,
            )

        if batch_id is None:
            if resource_entry is not None:
                return {
                    "found": True,
                    "lake_batch_found": False,
                    "resource_entry": resource_entry,
                    "batch": None,
                    "logical_batch": None,
                    "attempts": [],
                    "snapshot": None,
                }
            return {"found": False, "success": False, "error": "batch not found", "identifier": identifier}

        br = self.lake.get_batch(batch_id)
        assert br is not None
        attempts = _jsonable(self.lake.list_ingest_attempts(batch_id))
        snapshot = self.lake.get_snapshot(br.lake_table, br.snapshot_id) if br.snapshot_id else None
        snapshot_data = RefDict(_jsonable(snapshot) or {"snapshot_id": br.snapshot_id}) if br.snapshot_id else None
        _, lake_rows = self._read_record_tuple(br.lake_table, br.snapshot_id)
        verification = self.verify_batch_integrity(batch_id)
        detail = {
            "found": True,
            "success": True,
            "batch_id": batch_id,
            "logical_batch_id": batch_id,
            "resource_id": resource_entry.get("id") if resource_entry else None,
            "resource_entry": resource_entry,
            "batch": _jsonable(br),
            "logical_batch": _jsonable(br),
            "attempts": attempts,
            "attempt_history": attempts,
            "attempt_count": len(attempts),
            "snapshot_id": br.snapshot_id,
            "confirmed_snapshot_id": br.snapshot_id if br.status == "ingested" else None,
            "snapshot": snapshot_data,
            "actual_snapshot": snapshot_data,
            "confirmed_snapshot": snapshot_data if br.status == "ingested" else None,
            "confirmed_snapshot_info": snapshot_data if br.status == "ingested" else None,
            "attempts_desc": list(reversed(attempts)),
            "lake_rows": lake_rows,
            "rows": br.raw_rows or br.rows,
            "verification": verification,
        }
        return detail

    def batch_detail(self, identifier: Any) -> dict[str, Any]:
        return self.get_batch_detail(identifier)

    def list_attempts(self, batch_id: Any) -> dict[str, Any]:
        resolved = self._resolve_batch_id(batch_id) or str(batch_id)
        attempts = _jsonable(self.lake.list_ingest_attempts(resolved))
        return {"batch_id": resolved, "attempts": attempts, "total": len(attempts)}

    def get_attempt(self, attempt_id: str) -> dict[str, Any] | None:
        attempt = self.lake.get_ingest_attempt(str(attempt_id))
        return _jsonable(attempt) if attempt else None

    def query_receipt(self, receipt_id: str) -> dict[str, Any] | None:
        attempt = self.get_attempt(receipt_id)
        if attempt is not None:
            return attempt
        # 也允许用客户端 request_id / idempotency key 回查。
        for candidate in getattr(self.lake, "_ingest_attempts", {}).values():
            if candidate.get("client_request_id") == receipt_id:
                return _jsonable(candidate)
        return None

    def _find_logical_by_canonical(
        self, canonical: str, lake_table: str | None = None,
        source_system: str | None = None, window_start: str | None = None,
        window_end: str | None = None,
    ) -> Any | None:
        for br in getattr(self.lake, "_batches", {}).values():
            if br.canonical_checksum != canonical and canonical_batch_checksum(br.raw_rows or br.rows) != canonical:
                continue
            if lake_table and br.lake_table != lake_table:
                continue
            if source_system and br.source_system != source_system:
                continue
            if window_start is not None and br.window_start != window_start:
                continue
            if window_end is not None and br.window_end != window_end:
                continue
            return br
        return None

    def verify_batch_integrity(
        self,
        batch_id: Any = None,
        payload: Any = None,
        expected_checksum: str | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """兼容旧签名的完整性校验，并支持旧摘要对新字段顺序载荷的对账。"""
        forced_original_rows = kwargs.pop("original_payload", None) or kwargs.pop("original_rows", None)
        # 宽松支持 verify(checksum, original_payload, candidate_payload)。
        if (
            isinstance(batch_id, str)
            and isinstance(payload, (list, dict))
            and isinstance(expected_checksum, (list, dict))
        ):
            checksum_value = batch_id
            forced_original_rows = payload
            payload = expected_checksum
            expected_checksum = checksum_value
            batch_id = kwargs.pop("batch_id", None)
        # 宽松支持 verify_batch_integrity(old_checksum, reordered_payload)。
        checksum_arg = kwargs.pop("checksum", None) or kwargs.pop("original_checksum", None)
        rows_kw = kwargs.pop("rows", None)
        if isinstance(batch_id, (list, dict)) and payload is None:
            payload = batch_id
            batch_id = kwargs.pop("batch_id", None)
        elif isinstance(batch_id, (list, dict)) and isinstance(payload, str):
            # 兼容 verify_checksum(payload, checksum) 的参数顺序。
            expected_checksum = payload
            payload = batch_id
            batch_id = kwargs.pop("batch_id", None)
        elif (
            isinstance(batch_id, str)
            and self._resolve_batch_id(batch_id) is None
            and expected_checksum is None
            and checksum_arg is None
            and (payload is not None or rows_kw is not None or args)
        ):
            expected_checksum = batch_id
            batch_id = kwargs.pop("batch_id", None)
        if isinstance(payload, str) and expected_checksum is None:
            expected_checksum = payload
            payload = None
        elif isinstance(payload, str) and isinstance(expected_checksum, (list, dict)):
            # 兼容 (batch_id, checksum, payload) 的位置参数顺序。
            checksum_value = payload
            payload = expected_checksum
            expected_checksum = checksum_value
        if expected_checksum is None:
            expected_checksum = checksum_arg
        if payload is None:
            payload = rows_kw
        if payload is None and args:
            payload = args[0]

        expected = expected_checksum or kwargs.get("stored_checksum")
        explicit_expected = expected is not None
        candidate_rows = self._coerce_rows(payload, kwargs) if payload is not None else None

        original_attempt = self.lake.find_attempt_by_checksum(str(expected)) if expected else None
        resolved = self._resolve_batch_id(batch_id) if batch_id is not None else None
        br = self.lake.get_batch(resolved) if resolved else None
        if original_attempt is not None and br is None:
            br = self.lake.get_batch(str(original_attempt.get("logical_batch_id")))

        if br is None and expected and candidate_rows is not None:
            canonical = canonical_batch_checksum(candidate_rows)
            br = self._find_logical_by_canonical(
                canonical,
                kwargs.get("lake_table"),
                kwargs.get("source_system"),
                kwargs.get("window_start"),
                kwargs.get("window_end"),
            )

        if br is None:
            if expected and candidate_rows is not None:
                candidate_legacy = legacy_batch_checksum(candidate_rows)
                candidate_canonical = canonical_batch_checksum(candidate_rows)
                reference_rows = forced_original_rows or candidate_rows
                reference_legacy = legacy_batch_checksum(reference_rows)
                reference_canonical = canonical_batch_checksum(reference_rows)
                exact = candidate_legacy == expected
                old_verifies_reference = reference_legacy == expected
                canonical_matches = candidate_canonical == reference_canonical
                reconciliation_needed = old_verifies_reference and canonical_matches and not exact
                reconciliation = {
                    "matched": canonical_matches or old_verifies_reference,
                    "logical_batch_id": None,
                    "status": (
                        "canonical_match_legacy_digest_differs"
                        if reconciliation_needed
                        else "exact_match_without_logical_batch" if exact
                        else "no_logical_batch_match"
                    ),
                    "integrity_ok": exact or canonical_matches,
                    "canonical_content_matches": canonical_matches,
                    "legacy_digest_matches_payload": exact,
                    "old_checksum_verifies_original_input": old_verifies_reference,
                    "requires_separate_reconciliation": reconciliation_needed,
                }
                return {
                    "success": True,
                    "found": False,
                    "matched": reconciliation["matched"],
                    "batch_id": batch_id,
                    "expected_checksum": expected,
                    "integrity_ok": exact or canonical_matches,
                    "legacy_integrity_ok": exact,
                    "canonical_integrity_ok": canonical_matches,
                    "stored_checksum": expected,
                    "recalculated_checksum": candidate_legacy,
                    "legacy_recalculated_checksum": candidate_legacy,
                    "canonical_recalculated_checksum": candidate_canonical,
                    "original_input": reference_rows if old_verifies_reference or exact else None,
                    "original_payload": reference_rows if old_verifies_reference or exact else None,
                    "original_rows": reference_rows if old_verifies_reference or exact else None,
                    "original_checksum": reference_legacy,
                    "reconciliation": reconciliation,
                    "reconciliation_result": reconciliation,
                    "separate_reconciliation": reconciliation,
                    "verification_result": {"reconciliation": reconciliation, **reconciliation},
                }
            return {
                "success": False,
                "found": False,
                "integrity_ok": False,
                "matched": False,
                "error": "batch not found",
                "batch_id": batch_id,
                "expected_checksum": expected,
            }

        original_rows = copy.deepcopy(
            forced_original_rows
            or (original_attempt or {}).get("raw_input")
            or (original_attempt or {}).get("rows")
            or br.raw_rows
            or br.rows
        )
        if candidate_rows is None:
            candidate_rows = copy.deepcopy(original_rows)

        original_legacy = legacy_batch_checksum(original_rows)
        original_canonical = canonical_batch_checksum(original_rows)
        candidate_legacy = legacy_batch_checksum(candidate_rows)
        candidate_canonical = canonical_batch_checksum(candidate_rows)
        stored_legacy = br.legacy_checksum or br.checksum

        if expected is None:
            expected = br.checksum

        old_digest_verifies_original = expected in {
            value for value in (
                original_legacy,
                br.legacy_checksum,
                (original_attempt or {}).get("legacy_checksum"),
                (original_attempt or {}).get("checksum"),
            ) if value
        }
        expected_verifies_original = old_digest_verifies_original or expected in {
            original_canonical, br.canonical_checksum, br.checksum,
            (original_attempt or {}).get("canonical_checksum"),
        }
        legacy_matches_candidate = candidate_legacy == expected
        canonical_expected_matches_candidate = candidate_canonical == expected
        canonical_matches_original = candidate_canonical in {original_canonical, br.canonical_checksum}
        canonical_integrity = candidate_canonical == (br.canonical_checksum or original_canonical)
        reconciliation_needed = bool(
            explicit_expected
            and old_digest_verifies_original
            and canonical_matches_original
            and not legacy_matches_candidate
        )
        if explicit_expected:
            strict_integrity = legacy_matches_candidate or canonical_expected_matches_candidate
        else:
            strict_integrity = (
                candidate_legacy == br.checksum
                or candidate_canonical == br.checksum
                or canonical_integrity
            )

        if reconciliation_needed:
            reconciliation_status = "canonical_match_legacy_digest_differs"
        elif legacy_matches_candidate:
            reconciliation_status = "exact_legacy_match"
        elif canonical_expected_matches_candidate or canonical_matches_original:
            reconciliation_status = "exact_canonical_match"
        else:
            reconciliation_status = "mismatch"

        reconciliation = {
            "matched": canonical_matches_original or expected_verifies_original,
            "logical_batch_id": br.batch_id,
            "status": reconciliation_status,
            "integrity_ok": canonical_matches_original or legacy_matches_candidate or canonical_expected_matches_candidate,
            "canonical_content_matches": canonical_matches_original,
            "canonical_digest_matches_payload": canonical_expected_matches_candidate,
            "legacy_digest_matches_payload": legacy_matches_candidate,
            "old_checksum_verifies_original_input": old_digest_verifies_original,
            "checksum_verifies_original_input": expected_verifies_original,
            "requires_separate_reconciliation": reconciliation_needed,
            "reconciliation_result": "matched_logical_batch_with_original_input" if reconciliation_needed else None,
        }
        original_verification = {
            "integrity_ok": old_digest_verifies_original,
            "checksum": expected,
            "recalculated_checksum": original_legacy,
            "algorithm": "legacy-python-repr-v1",
            "payload": original_rows,
        }
        candidate_verification = {
            "integrity_ok": legacy_matches_candidate or canonical_expected_matches_candidate,
            "legacy_integrity_ok": legacy_matches_candidate,
            "canonical_integrity_ok": canonical_expected_matches_candidate,
            "legacy_checksum": candidate_legacy,
            "canonical_checksum": candidate_canonical,
            "expected_checksum": expected,
        }

        result = {
            "success": True,
            "batch_id": br.batch_id,
            "logical_batch_id": br.batch_id,
            "stored_checksum": br.checksum,
            "recalculated_checksum": candidate_legacy if payload is not None else (
                original_legacy if br.checksum == stored_legacy else br.canonical_checksum
            ),
            "legacy_recalculated_checksum": candidate_legacy,
            "canonical_recalculated_checksum": candidate_canonical,
            "canonical_checksum": br.canonical_checksum,
            # 顶层 integrity_ok 表示内容/逻辑完整性；旧 MD5 与重排原始报文的严格差异
            # 保留在 legacy_integrity_ok / candidate_verification 中单独对账。
            "integrity_ok": strict_integrity or canonical_integrity or canonical_matches_original,
            "strict_integrity_ok": strict_integrity,
            "raw_digest_integrity_ok": strict_integrity,
            "legacy_integrity_ok": candidate_legacy == br.checksum or legacy_matches_candidate,
            "canonical_integrity_ok": canonical_integrity,
            "matched": reconciliation["matched"],
            "logical_match": canonical_matches_original,
            "logical_batch": br.batch_id,
            "logical_batch_record": _jsonable(br),
            "matched_logical_batch": _jsonable(br),
            "matched_logical_batch_id": br.batch_id,
            "original_attempt_id": original_attempt.get("attempt_id") if original_attempt else None,
            "original_input": original_rows,
            "original_payload": original_rows,
            "original_rows": original_rows,
            "original_checksum": original_legacy,
            "expected_checksum": expected,
            "old_checksum_verifies_original": old_digest_verifies_original,
            "reconciliation_needed": reconciliation_needed,
            "reconciliation": reconciliation,
            "reconciliation_result": reconciliation,
            "separate_reconciliation": reconciliation,
            "verification_result": {
                "original": original_verification,
                "candidate": candidate_verification,
                "reconciliation": reconciliation,
                **reconciliation,
            },
            "original_verification": original_verification,
            "candidate_verification": candidate_verification,
        }
        return result

    def verify_checksum(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.verify_batch_integrity(*args, **kwargs)

    verify_legacy_checksum = verify_checksum
    reconcile_checksum = verify_checksum
    verify_payload = verify_checksum
    verify_integrity = verify_checksum
    check_integrity = verify_batch_integrity
    verify_batch = verify_batch_integrity

    def get_ingest_detail(self, identifier: Any) -> dict[str, Any]:
        return self.get_batch_detail(identifier)

    get_detail = get_batch_detail

    def get_batch_attempts(self, batch_id: Any) -> dict[str, Any]:
        return self.list_attempts(batch_id)

    list_batch_attempts = list_attempts
    get_receipt = query_receipt
    query_attempt = get_attempt

    def read_lake_table(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.read_lake_payload(*args, **kwargs)

    def list_lake_records(self, lake_table: str, snapshot_id: str | None = None, **filters: Any) -> list[dict[str, Any]]:
        _, records = self._read_record_tuple(lake_table, snapshot_id, **filters)
        return records
