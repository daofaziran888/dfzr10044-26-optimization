"""湖表存储引擎：文件注册表、分区清单、快照链、水位存储与批次数据。

本模块提供跨服务共享的不可变数据层：
- 文件注册：每个分区文件有唯一 file_id、所属分区、所属湖表、行数、字节数
- 快照链：每个湖表按时间线记录快照，含 file_ids、manifest、父快照指针
- 水位：每张湖表的已提交/待提交水位记录
- 批次数据：入湖批次的行级数据存储，含校验值
- 隔离区：被质量规则拦截的数据行，按批次隔离
- 写锁：并发提交冲突检测

设计原则：
- 所有写操作在提交前必须拿到全局锁
- 快照只在事务完整落湖后标记为可用
- 归并时先写入新文件清单，再原子地替换旧文件列表
"""
from __future__ import annotations

import copy
import hashlib
import json
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any


def legacy_batch_checksum(rows: list[dict[str, Any]]) -> str:
    """升级前使用的摘要算法：依赖 dict 的原始键顺序和 Python repr。

    必须原样保留，才能验证升级前已经保存的摘要和原始载荷。
    """
    return hashlib.md5(
        str(sorted([str(r) for r in rows])).encode()
    ).hexdigest()


def canonical_json_value(value: Any) -> str:
    """与字段顺序无关的规范 JSON；无法 JSON 化时退化为稳定 repr。"""
    try:
        return json.dumps(
            value, ensure_ascii=False, sort_keys=True,
            separators=(",", ":"), default=str,
        )
    except (TypeError, ValueError):
        return repr(value)


def row_content_checksum(row: dict[str, Any]) -> str:
    """单行内容摘要：dict 字段重排不会改变结果。"""
    return hashlib.md5(canonical_json_value(row).encode("utf-8")).hexdigest()


def canonical_batch_checksum(rows: list[dict[str, Any]]) -> str:
    """整批行集合的规范摘要。

    先规范化每一行，再按行摘要排序，因此字段顺序和传输行顺序均不影响逻辑内容。
    """
    row_digests = sorted(row_content_checksum(row) for row in rows)
    return hashlib.md5("\n".join(row_digests).encode("utf-8")).hexdigest()


# ----------------------------- 文件注册 -----------------------------

@dataclass
class PartitionFile:
    """分区文件：每个文件属于一个分区。"""
    file_id: str
    lake_table: str
    partition_key: str
    file_rows: int
    file_bytes: int
    file_manifest: str  # 文件内容摘要（MD5）
    created_at: float


# ----------------------------- 快照 -----------------------------

@dataclass
class LakeSnapshot:
    """湖表快照：记录某个时刻整个湖表的分区文件清单。"""
    snapshot_id: str
    lake_table: str
    parent_snapshot: str | None
    file_ids: list[str]
    added_rows: int
    deleted_rows: int
    manifest_checksum: str
    status: str  # generating | available | expired | rolled_back
    created_at: float


# ----------------------------- 水位 -----------------------------

@dataclass
class WatermarkRecord:
    """入湖水位追踪：记录已提交和待提交水位。"""
    lake_table: str
    field_name: str
    committed_value: str
    pending_value: str | None
    strategy: str  # append | merge | overwrite
    last_advanced_at: float


# ----------------------------- 批次数据 -----------------------------

@dataclass
class BatchRecord:
    """入湖批次：一次入湖操作导出的一组数据行。"""
    batch_id: str
    lake_table: str
    source_system: str
    rows: list[dict[str, Any]]  # 实际数据行
    row_count: int
    window_start: str
    window_end: str
    checksum: str
    snapshot_id: str | None  # 落湖后分配的 snap
    status: str  # pending | ingesting | ingested | failed
    created_at: float
    canonical_checksum: str = ""
    legacy_checksum: str = ""
    checksum_version: str = "legacy-python-repr-v1"
    logical_batch_id: str = ""
    raw_rows: list[dict[str, Any]] = field(default_factory=list)


# ----------------------------- 写锁 -----------------------------

class WriteLock:
    """全局写入锁：同一湖表同时只能有一个写入者提交。"""

    def __init__(self) -> None:
        self._locks: dict[str, threading.Lock] = {}
        self._lock = threading.Lock()

    def acquire(self, lake_table: str) -> threading.Lock:
        with self._lock:
            if lake_table not in self._locks:
                self._locks[lake_table] = threading.Lock()
            table_lock = self._locks[lake_table]
        table_lock.acquire()
        return table_lock

    def release(self, lake_table: str) -> None:
        with self._lock:
            lock = self._locks.get(lake_table)
        if lock is not None:
            try:
                lock.release()
            except RuntimeError:
                pass


# ----------------------------- 存储引擎 -----------------------------

class LakeStore:
    """湖表存储引擎主类。

    管理三大维度：
    - files: 分区文件注册表
    - snapshots: 快照链
    - watermarks: 水位
    - batches: 批次数据
    - quarantine: 隔离区数据
    - write_lock: 并发提交的控制锁
    """

    def __init__(self) -> None:
        # 文件注册表：lake_table -> {file_id -> PartitionFile}
        self._files: dict[str, dict[str, PartitionFile]] = {}

        # 快照链：lake_table -> list[LakeSnapshot]（时间序）
        self._snapshots: dict[str, list[LakeSnapshot]] = {}

        # 水位：lake_table -> WatermarkRecord
        self._watermarks: dict[str, WatermarkRecord] = {}

        # 批次数据：batch_id -> BatchRecord
        self._batches: dict[str, BatchRecord] = {}

        # 逻辑入湖批次与每次传输尝试。物理 BatchRecord 保留首包原始摘要，
        # attempts 保留重传/整组补传的每一次原始载荷和核验线索。
        self._ingest_attempts: dict[str, dict[str, Any]] = {}
        self._batch_attempt_ids: dict[str, list[str]] = {}
        self._identity_to_logical: dict[tuple[str, str, str, str, str], str] = {}
        self._item_owner: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}

        # 已确认入湖的行记录与快照行清单。key 分别为 table:row_uid 和
        # (table, snapshot_id)，保证重复确认不会在快照中产生第二份 A。
        self._lake_rows: dict[str, dict[str, dict[str, Any]]] = {}
        self._snapshot_row_uids: dict[tuple[str, str], list[str]] = {}
        self._ingest_lock = threading.RLock()

        # 隔离区：batch_id -> list[dict]（被拦截的数据行）
        self._quarantine: dict[str, list[dict[str, Any]]] = {}

        # 写锁
        self._write_lock = WriteLock()

        # 归并中断恢复检查点：merge_id -> {partial_file_ids: [...], ...}
        self._merge_checkpoints: dict[str, dict[str, Any]] = {}

    # ---- 文件操作 ----

    def register_file(
        self, lake_table: str, partition_key: str, rows: int, bytes_: int, manifest: str | None = None
    ) -> PartitionFile:
        file_id = f"{lake_table}-p{partition_key}-{uuid.uuid4().hex[:8]}"
        checksum = manifest or hashlib.md5(f"{lake_table}:{partition_key}:{time.time()}".encode()).hexdigest()
        pf = PartitionFile(
            file_id=file_id,
            lake_table=lake_table,
            partition_key=partition_key,
            file_rows=rows,
            file_bytes=bytes_,
            file_manifest=checksum,
            created_at=time.time(),
        )
        self._files.setdefault(lake_table, {})[file_id] = pf
        return pf

    def list_files(self, lake_table: str, partition_key: str | None = None) -> list[PartitionFile]:
        files = list(self._files.get(lake_table, {}).values())
        if partition_key is not None:
            files = [f for f in files if f.partition_key == partition_key]
        return files

    def get_file(self, lake_table: str, file_id: str) -> PartitionFile | None:
        return self._files.get(lake_table, {}).get(file_id)

    def remove_files(self, lake_table: str, file_ids: list[str]) -> int:
        table_files = self._files.get(lake_table, {})
        removed = 0
        for fid in file_ids:
            if fid in table_files:
                del table_files[fid]
                removed += 1
        return removed

    def replace_file_list(
        self, lake_table: str, old_file_ids: list[str], new_files: list[PartitionFile]
    ) -> None:
        """原子替换：先注册新文件，再删除旧文件。保证归并不留半份清单。"""
        table_files = self._files.setdefault(lake_table, {})
        for nf in new_files:
            table_files[nf.file_id] = nf
        for fid in old_file_ids:
            table_files.pop(fid, None)

    # ---- 快照操作 ----

    def create_snapshot(
        self,
        lake_table: str,
        file_ids: list[str],
        parent_snapshot: str | None = None,
        added_rows: int = 0,
        deleted_rows: int = 0,
    ) -> LakeSnapshot:
        snap_id = f"snap-{lake_table}-{uuid.uuid4().hex[:8]}"
        manifest = hashlib.md5(
            ",".join(sorted(file_ids)).encode()
        ).hexdigest()
        snap = LakeSnapshot(
            snapshot_id=snap_id,
            lake_table=lake_table,
            parent_snapshot=parent_snapshot,
            file_ids=list(file_ids),
            added_rows=added_rows,
            deleted_rows=deleted_rows,
            manifest_checksum=manifest,
            status="generating",
            created_at=time.time(),
        )
        chain = self._snapshots.setdefault(lake_table, [])
        chain.append(snap)
        return snap

    def mark_snapshot_available(self, lake_table: str, snapshot_id: str) -> LakeSnapshot | None:
        for snap in self._snapshots.get(lake_table, []):
            if snap.snapshot_id == snapshot_id:
                snap.status = "available"
                return snap
        return None

    def mark_snapshot_expired(self, lake_table: str, snapshot_id: str) -> LakeSnapshot | None:
        for snap in self._snapshots.get(lake_table, []):
            if snap.snapshot_id == snapshot_id:
                snap.status = "expired"
                return snap
        return None

    def get_snapshot(self, lake_table: str, snapshot_id: str) -> LakeSnapshot | None:
        for snap in self._snapshots.get(lake_table, []):
            if snap.snapshot_id == snapshot_id:
                return snap
        return None

    def list_snapshots(self, lake_table: str) -> list[LakeSnapshot]:
        return list(self._snapshots.get(lake_table, []))

    def get_snapshot_at_time(self, lake_table: str, timestamp: float) -> LakeSnapshot | None:
        """时间旅行：返回 <=timestamp 的最近可用快照。"""
        candidates = [
            s for s in self._snapshots.get(lake_table, [])
            if s.status == "available" and s.created_at <= timestamp
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda s: s.created_at)

    # ---- 水位操作 ----

    def set_watermark(
        self, lake_table: str, field_name: str, committed: str,
        strategy: str = "append"
    ) -> WatermarkRecord:
        wm = WatermarkRecord(
            lake_table=lake_table,
            field_name=field_name,
            committed_value=committed,
            pending_value=None,
            strategy=strategy,
            last_advanced_at=time.time(),
        )
        self._watermarks[lake_table] = wm
        return wm

    def advance_watermark(self, lake_table: str, new_value: str) -> WatermarkRecord | None:
        wm = self._watermarks.get(lake_table)
        if wm is None:
            return None
        wm.pending_value = new_value
        return wm

    def commit_watermark(self, lake_table: str) -> WatermarkRecord | None:
        wm = self._watermarks.get(lake_table)
        if wm is None or wm.pending_value is None:
            return None
        wm.committed_value = wm.pending_value
        wm.pending_value = None
        wm.last_advanced_at = time.time()
        return wm

    def get_watermark(self, lake_table: str) -> WatermarkRecord | None:
        return self._watermarks.get(lake_table)

    def list_watermarks(self) -> list[WatermarkRecord]:
        return list(self._watermarks.values())

    # ---- 批次数据 ----

    def store_batch(
        self, batch_id: str, lake_table: str, source_system: str,
        rows: list[dict[str, Any]], window_start: str, window_end: str,
    ) -> BatchRecord:
        checksum = legacy_batch_checksum(rows)
        canonical = canonical_batch_checksum(rows)
        br = BatchRecord(
            batch_id=batch_id,
            lake_table=lake_table,
            source_system=source_system,
            rows=list(rows),
            row_count=len(rows),
            window_start=window_start,
            window_end=window_end,
            checksum=canonical,
            snapshot_id=None,
            status="pending",
            created_at=time.time(),
            canonical_checksum=canonical,
            legacy_checksum=checksum,
            checksum_version="canonical-json-v2",
            logical_batch_id=batch_id,
            raw_rows=copy.deepcopy(list(rows)),
        )
        self._batches[batch_id] = br
        self._batch_attempt_ids.setdefault(batch_id, [])
        return br

    def get_batch(self, batch_id: str) -> BatchRecord | None:
        return self._batches.get(batch_id)

    def list_batches(self, lake_table: str) -> list[BatchRecord]:
        return [b for b in self._batches.values() if b.lake_table == lake_table]

    def update_batch_status(self, batch_id: str, status: str, snapshot_id: str | None = None) -> BatchRecord | None:
        br = self._batches.get(batch_id)
        if br is None:
            return None
        br.status = status
        if snapshot_id is not None:
            br.snapshot_id = snapshot_id
        return br

    # ---- 逻辑批次、尝试与幂等入湖 ----

    @staticmethod
    def logical_identity(
        lake_table: str, source_system: str, window_start: str,
        window_end: str, content_checksum: str,
    ) -> tuple[str, str, str, str, str]:
        return (
            str(lake_table), str(source_system), str(window_start),
            str(window_end), str(content_checksum),
        )

    def find_logical_batch(self, identity: tuple[str, str, str, str, str]) -> BatchRecord | None:
        batch_id = self._identity_to_logical.get(identity)
        return self._batches.get(batch_id) if batch_id else None

    def register_logical_identity(
        self, identity: tuple[str, str, str, str, str], batch_id: str,
    ) -> None:
        self._identity_to_logical.setdefault(identity, batch_id)

    def save_ingest_attempt(self, attempt: dict[str, Any]) -> dict[str, Any]:
        attempt_id = str(attempt["attempt_id"])
        self._ingest_attempts[attempt_id] = attempt
        batch_id = str(attempt.get("logical_batch_id") or attempt.get("batch_id") or "")
        if batch_id:
            self._batch_attempt_ids.setdefault(batch_id, []).append(attempt_id)
        return attempt

    def get_ingest_attempt(self, attempt_id: str) -> dict[str, Any] | None:
        return self._ingest_attempts.get(attempt_id)

    def list_ingest_attempts(self, batch_id: str) -> list[dict[str, Any]]:
        ids = self._batch_attempt_ids.get(batch_id, [])
        attempts = [self._ingest_attempts[aid] for aid in ids if aid in self._ingest_attempts]
        # 兼容 attempt 中同时把物理/逻辑编号写到 batch_id 的情况。
        extra = [
            a for a in self._ingest_attempts.values()
            if (a.get("logical_batch_id") == batch_id or a.get("batch_id") == batch_id)
            and a.get("attempt_id") not in {x.get("attempt_id") for x in attempts}
        ]
        return sorted(attempts + extra, key=lambda a: float(a.get("created_at") or 0))

    def find_attempt_by_checksum(self, checksum: str) -> dict[str, Any] | None:
        for attempt in self._ingest_attempts.values():
            if attempt.get("checksum") == checksum or attempt.get("legacy_checksum") == checksum:
                return attempt
        return None

    def remember_item_owner(
        self,
        item_key: tuple[str, str, str, str, str],
        owner: dict[str, Any],
    ) -> dict[str, Any]:
        return self._item_owner.setdefault(item_key, owner)

    def get_item_owner(
        self, item_key: tuple[str, str, str, str, str],
    ) -> dict[str, Any] | None:
        return self._item_owner.get(item_key)

    def commit_lake_records(
        self,
        lake_table: str,
        records: list[dict[str, Any]],
        snapshot_id: str | None = None,
    ) -> tuple[str, list[dict[str, Any]], LakeSnapshot | None]:
        """把逻辑行记录提交到湖表；相同 row_uid 只保留一份。"""
        table_rows = self._lake_rows.setdefault(lake_table, {})
        newly_added: list[dict[str, Any]] = []
        for record in records:
            uid = str(record["row_uid"])
            if uid not in table_rows:
                table_rows[uid] = record
                newly_added.append(record)

        existing_snapshot = self.get_snapshot(lake_table, snapshot_id) if snapshot_id else None
        if snapshot_id and existing_snapshot is None:
            # 调用方可能使用外部/业务侧已有快照号；登记为可用快照，便于详情和读取回查。
            previous_chain = self.list_snapshots(lake_table)
            previous_snapshot = previous_chain[-1] if previous_chain else None
            file_ids = list(previous_snapshot.file_ids) if previous_snapshot else []
            if newly_added:
                pf = self.register_file(
                    lake_table,
                    "ingest_batch",
                    rows=len(newly_added),
                    bytes_=sum(
                        len(canonical_json_value(r.get("values", {})).encode())
                        for r in newly_added
                    ),
                )
                file_ids.append(pf.file_id)
            snap = LakeSnapshot(
                snapshot_id=snapshot_id,
                lake_table=lake_table,
                parent_snapshot=previous_snapshot.snapshot_id if previous_snapshot else None,
                file_ids=file_ids,
                added_rows=len(newly_added),
                deleted_rows=0,
                manifest_checksum=hashlib.md5(snapshot_id.encode()).hexdigest(),
                status="available",
                created_at=time.time(),
            )
            self._snapshots.setdefault(lake_table, []).append(snap)
            existing_snapshot = snap

        if existing_snapshot is not None:
            snap = existing_snapshot
            if snap.status != "available":
                self.mark_snapshot_available(lake_table, snap.snapshot_id)
        else:
            previous_chain = self.list_snapshots(lake_table)
            previous = previous_chain[-1] if previous_chain else None
            file_ids = list(previous.file_ids) if previous else []
            if newly_added:
                pf = self.register_file(
                    lake_table,
                    "ingest_batch",
                    rows=len(newly_added),
                    bytes_=sum(len(canonical_json_value(r.get("values", {})).encode()) for r in newly_added),
                )
                file_ids.append(pf.file_id)
            snap = self.create_snapshot(
                lake_table,
                file_ids=file_ids,
                parent_snapshot=previous.snapshot_id if previous else None,
                added_rows=len(newly_added),
            )
            self.mark_snapshot_available(lake_table, snap.snapshot_id)

        uids = list(table_rows.keys())
        self._snapshot_row_uids[(lake_table, snap.snapshot_id)] = uids
        return snap.snapshot_id, [table_rows[uid] for uid in uids], snap

    def read_lake_records(
        self, lake_table: str, snapshot_id: str | None = None,
    ) -> tuple[str | None, list[dict[str, Any]]]:
        table_rows = self._lake_rows.get(lake_table, {})
        if snapshot_id is None:
            available = [s for s in self.list_snapshots(lake_table) if s.status == "available"]
            if not available:
                return None, []
            snapshot_id = available[-1].snapshot_id
        uids = self._snapshot_row_uids.get((lake_table, snapshot_id))
        if uids is None:
            snap = self.get_snapshot(lake_table, snapshot_id)
            if snap is None:
                return snapshot_id, []
            # 非本接口登记的快照没有行级清单；不猜测内容，返回空列表。
            uids = []
        return snapshot_id, [copy.deepcopy(table_rows[uid]) for uid in uids if uid in table_rows]

    # ---- 隔离区 ----

    def quarantine_rows(
        self, batch_id: str, rule_id: str, rule_name: str,
        rows: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """将被质量规则拦截的行放入隔离区。"""
        quarantined = []
        for row in rows:
            qr = dict(row)
            qr["_quarantine_batch"] = batch_id
            qr["_quarantine_rule"] = rule_id
            qr["_quarantine_rule_name"] = rule_name
            qr["_quarantine_time"] = time.time()
            quarantined.append(qr)
        self._quarantine.setdefault(batch_id, []).extend(quarantined)
        return quarantined

    def list_quarantine(self, batch_id: str | None = None) -> list[dict[str, Any]]:
        if batch_id is not None:
            return list(self._quarantine.get(batch_id, []))
        result: list[dict[str, Any]] = []
        for rows in self._quarantine.values():
            result.extend(rows)
        return result

    def release_quarantine(self, batch_id: str) -> list[dict[str, Any]]:
        """回炉重入：从隔离区取出数据行。"""
        rows = self._quarantine.pop(batch_id, [])
        cleaned: list[dict[str, Any]] = []
        for r in rows:
            cleaned_row = {k: v for k, v in r.items() if not k.startswith("_quarantine_")}
            cleaned.append(cleaned_row)
        return cleaned

    # ---- 归并检查点 ----

    def save_merge_checkpoint(self, merge_id: str, partial_file_ids: list[str]) -> None:
        self._merge_checkpoints[merge_id] = {
            "partial_file_ids": list(partial_file_ids),
            "saved_at": time.time(),
        }

    def get_merge_checkpoint(self, merge_id: str) -> dict[str, Any] | None:
        return self._merge_checkpoints.get(merge_id)

    def clear_merge_checkpoint(self, merge_id: str) -> None:
        self._merge_checkpoints.pop(merge_id, None)

    # ---- 并发写锁 ----

    def acquire_write_lock(self, lake_table: str) -> threading.Lock:
        return self._write_lock.acquire(lake_table)

    def release_write_lock(self, lake_table: str) -> None:
        self._write_lock.release(lake_table)

    # ---- 统计快照 ----

    def stats_snapshot(self, lake_table: str) -> dict[str, Any]:
        """生成湖表统计画像快照。"""
        files = self.list_files(lake_table)
        snapshots = self.list_snapshots(lake_table)
        wm = self.get_watermark(lake_table)
        total_rows = sum(f.file_rows for f in files)
        total_bytes = sum(f.file_bytes for f in files)
        partition_count = len(set(f.partition_key for f in files))
        return {
            "lake_table": lake_table,
            "file_count": len(files),
            "total_rows": total_rows,
            "total_bytes": total_bytes,
            "partition_count": partition_count,
            "available_snapshots": sum(1 for s in snapshots if s.status == "available"),
            "watermark": wm.committed_value if wm else None,
        }


# 全局单例
lake_store = LakeStore()