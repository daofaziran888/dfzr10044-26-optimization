"""时间旅行领域服务：按快照/时间点回溯查询历史湖表数据。"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store
from app.lake_store import lake_store


class TimeTravelService(BaseModuleService):
    MODULE = "time_travel"
    ENTITY = "回溯查询"
    OPERATION_TERM = "读取"
    LIST_FIELDS = ["查询编号", "所属湖表", "目标快照", "回溯时间点", "查询列", "返回行数", "命中分区数", "查询耗时"]
    REQUIRED_FIELDS = ["查询编号", "所属湖表", "目标快照"]
    STATUS_ORDER = ["待执行", "执行中", "已返回", "执行失败"]
    ACTION_RULES = {"发起查询": "执行中", "取消查询": "已返回", "标记失败": "执行失败"}
    NEGATIVE_ACTIONS = []

    def read_at_snapshot(self, lake_table: str, snapshot_id: str,
                         columns: list[str] | None = None) -> dict[str, Any]:
        """按指定快照 ID 读取历史数据并返回命中分区数和行数。"""
        snap = lake_store.get_snapshot(lake_table, snapshot_id)
        if snap is None:
            return {"success": False, "error": "snapshot not found", "rows": 0, "partitions": 0}
        total_rows = 0
        partitions = set()
        for fid in snap.file_ids:
            pf = lake_store.get_file(lake_table, fid)
            if pf:
                total_rows += pf.file_rows
                partitions.add(pf.partition_key)
        _, stored_records = lake_store.read_lake_records(lake_table, snapshot_id)
        record_items = []
        for record in stored_records:
            item = dict(record.get("values") or {})
            item.update({
                "row_uid": record.get("row_uid"),
                "source_system": record.get("source_system"),
                "source_credential": record.get("source_credential"),
                "accepted_scope": record.get("accepted_scope"),
                "accepted_range": record.get("accepted_range") or record.get("accepted_scope"),
                "accepted_ranges": record.get("accepted_ranges"),
                "snapshot_id": snapshot_id,
            })
            record_items.append(item)
        return {
            "success": True,
            "snapshot_id": snapshot_id,
            "rows": total_rows,
            "row_count": total_rows,
            "records": stored_records,
            "items": record_items,
            "data": record_items,
            "partition_count": len(partitions),
            "partitions_hit": sorted(partitions),
            "query_time": time.time(),
        }

    def read_at_timepoint(self, lake_table: str, timestamp: float) -> dict[str, Any]:
        """按时间点回溯查找最近可用快照并读取。"""
        snap = lake_store.get_snapshot_at_time(lake_table, timestamp)
        if snap is None:
            return {"success": False, "error": "no available snapshot at time", "rows": 0, "partitions": 0}
        return self.read_at_snapshot(lake_table, snap.snapshot_id)

    def compare_two_snapshots(self, lake_table: str, snap_a: str, snap_b: str) -> dict[str, Any]:
        """比较两个快照间的数据差异。"""
        s_a = lake_store.get_snapshot(lake_table, snap_a)
        s_b = lake_store.get_snapshot(lake_table, snap_b)
        if not s_a or not s_b:
            return {"success": False, "error": "snapshot not found"}
        return {
            "success": True,
            "snap_a": snap_a,
            "snap_b": snap_b,
            "diff_added_rows": max(0, s_b.added_rows - s_a.added_rows),
            "diff_deleted_rows": max(0, s_b.deleted_rows - s_a.deleted_rows),
        }