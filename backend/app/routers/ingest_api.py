"""入湖批次幂等重传、整组补传、详情和核验的专用 HTTP 接口。

通用资源路由仍保留 /api/ingest_batch 的列表/登记/动作契约；这里补齐场景化接口，
并同时提供下划线/连字符路径别名，方便既有调用方和新对账页面接入。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request

from app.services import service_for
from app.services.ingest_batch import IngestBatchService, _jsonable
from app.store import store

router = APIRouter(prefix="/api", tags=["入湖批次幂等与核验"])


def svc() -> IngestBatchService:
    service = service_for("ingest_batch")
    assert isinstance(service, IngestBatchService)
    return service


async def _json(request: Request) -> dict[str, Any]:
    try:
        data = await request.json()
    except Exception:
        return {}
    return data if isinstance(data, dict) else {"payload": data}


def _pick(data: dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in data and data[key] is not None:
            return data[key]
    return default


@router.post("/ingest_batch/receive")
@router.post("/ingest-batch/receive")
@router.post("/ingest-batches/receive")
@router.post("/ingest_batch/ingest")
@router.post("/ingest_batch/batch")
@router.post("/ingest_batch/submit")
@router.post("/ingest-batch/submit")
@router.post("/ingest-batches")
async def receive(request: Request) -> dict[str, Any]:
    data = await _json(request)
    data.setdefault("source_credential", request.headers.get("x-source-credential"))
    data.setdefault("request_id", request.headers.get("x-request-id") or request.headers.get("idempotency-key"))
    data["register_resource"] = True
    if not data.get("source_system") and request.headers.get("x-source-system"):
        data["source_system"] = request.headers["x-source-system"]
    return svc().ingest_batch(
        lake_table=_pick(data, "lake_table", "table", "所属湖表"),
        source_system=_pick(data, "source_system", "source", "来源系统"),
        rows=_pick(data, "rows", "items", "records", "payload", "数据行", "行数据", "明细"),
        window_start=_pick(data, "window_start", "start", "窗口开始"),
        window_end=_pick(data, "window_end", "end", "窗口结束"),
        **{k: v for k, v in data.items() if k not in {
            "lake_table", "table", "所属湖表", "source_system", "source", "来源系统",
            "rows", "items", "records", "payload", "window_start", "start", "window_end", "end",
        }},
    )


@router.post("/ingest_batch/retransmit")
@router.post("/ingest-batch/retransmit")
@router.post("/ingest_batch/retry")
@router.post("/ingest_batch/reupload")
@router.post("/ingest_batch/retransmit-group")
@router.post("/ingest-batch/retransmit-group")
@router.post("/ingest_batch/group-retransmit")
@router.post("/ingest_batch/batch-retransmit")
@router.post("/ingest-batch/batch-retransmit")
@router.post("/ingest-batches/retransmit")
@router.post("/ingest-batches/group-retransmit")
@router.post("/ingest/retransmit")
@router.post("/ingest/group-retransmit")
async def retransmit(request: Request) -> dict[str, Any]:
    data = await _json(request)
    data.setdefault("source_credential", request.headers.get("x-source-credential"))
    data.setdefault("request_id", request.headers.get("x-request-id") or request.headers.get("idempotency-key"))
    data["register_resource"] = True
    if not data.get("source_system") and request.headers.get("x-source-system"):
        data["source_system"] = request.headers["x-source-system"]
    return _jsonable(svc().retransmit_group(data))


@router.post("/ingest_batch/verify")
@router.post("/ingest-batch/verify")
@router.post("/ingest_batch/verify-integrity")
@router.post("/ingest-batch/verify-integrity")
@router.post("/ingest_batch/integrity")
@router.post("/ingest-batches/verify")
async def verify(request: Request) -> dict[str, Any]:
    data = await _json(request)
    batch_id = _pick(data, "batch_id", "id", "logical_batch_id")
    checksum = _pick(data, "checksum", "expected_checksum", "original_checksum", "stored_checksum")
    payload = _pick(data, "payload", "rows", "items", "original_payload")
    return svc().verify_batch_integrity(
        batch_id,
        payload,
        checksum,
        lake_table=_pick(data, "lake_table", "table"),
        source_system=_pick(data, "source_system", "source"),
        window_start=_pick(data, "window_start"),
        window_end=_pick(data, "window_end"),
    )


@router.get("/ingest_batch/verify")
async def verify_get(
    batch_id: str | None = None,
    checksum: str | None = None,
    lake_table: str | None = None,
    source_system: str | None = None,
    window_start: str | None = None,
    window_end: str | None = None,
) -> dict[str, Any]:
    return svc().verify_batch_integrity(
        batch_id,
        None,
        checksum,
        lake_table=lake_table,
        source_system=source_system,
        window_start=window_start,
        window_end=window_end,
    )


@router.get("/ingest_batch/read")
@router.get("/ingest-batch/read")
async def read_alias(
    lake_table: str | None = None,
    table: str | None = None,
    snapshot_id: str | None = None,
    source_system: str | None = None,
    source: str | None = None,
) -> dict[str, Any]:
    return svc().read_lake_payload(
        lake_table or table or "",
        snapshot_id,
        source_system=source_system or source,
    )


@router.get("/ingest_batch/detail/{batch_id}")
@router.get("/ingest-batch/detail/{batch_id}")
@router.get("/ingest_batch/details/{batch_id}")
@router.get("/ingest-batches/{batch_id}/detail")
@router.get("/ingest-batches/{batch_id}")
async def detail(batch_id: str) -> dict[str, Any]:
    return svc().get_batch_detail(batch_id)


@router.get("/ingest_batch/{batch_id}")
async def resource_or_detail(batch_id: str) -> dict[str, Any]:
    # 数字 ID 保持通用资源接口的直接 entry 契约；逻辑批次字符串返回富详情。
    if batch_id.isdigit():
        entry = store.find("ingest_batch", int(batch_id))
        if entry is not None:
            return entry
    return svc().get_batch_detail(batch_id)


@router.get("/ingest_batch/{batch_id}/detail")
async def batch_detail_alias(batch_id: str) -> dict[str, Any]:
    return svc().get_batch_detail(batch_id)


@router.get("/ingest_batch/{batch_id}/attempts")
async def batch_attempts(batch_id: str) -> dict[str, Any]:
    return svc().list_attempts(batch_id)


@router.get("/ingest_batch/attempt/{attempt_id}")
@router.get("/ingest_batch/receipt/{attempt_id}")
async def get_attempt(attempt_id: str) -> dict[str, Any]:
    attempt = svc().query_receipt(attempt_id)
    if attempt is None:
        return {"found": False, "success": False, "error": "attempt not found"}
    return {"found": True, "success": True, "attempt": attempt}


@router.post("/ingest_batch/{batch_id}/confirm")
@router.post("/ingest_batch/{batch_id}/promote")
@router.post("/ingest-batches/{batch_id}/confirm")
async def confirm(batch_id: str, request: Request) -> dict[str, Any]:
    data = await _json(request)
    snapshot_id = _pick(data, "snapshot_id", "snapshotId", "snapshot")
    return svc().confirm_ingestion(batch_id, snapshot_id)


@router.post("/ingest_batch/{batch_id}/verify")
async def verify_by_batch(batch_id: str, request: Request) -> dict[str, Any]:
    data = await _json(request)
    checksum = _pick(data, "checksum", "expected_checksum", "original_checksum")
    payload = _pick(data, "payload", "rows", "items")
    return svc().verify_batch_integrity(batch_id, payload, checksum)


@router.post("/ingest_batch/{batch_id}/retransmit")
async def retransmit_one(batch_id: str, request: Request) -> dict[str, Any]:
    data = await _json(request)
    data.setdefault("logical_batch_id", batch_id)
    return _jsonable(svc().retransmit_group(data))


@router.get("/ingest_batch/read/rows")
@router.get("/ingest-batch/read/rows")
@router.get("/ingest_batch/lake/read")
@router.get("/lake/ingest-rows")
async def read_rows(
    lake_table: str | None = None,
    table: str | None = None,
    snapshot_id: str | None = None,
    source_system: str | None = None,
    source: str | None = None,
) -> dict[str, Any]:
    return svc().read_lake_payload(
        lake_table or table or "",
        snapshot_id,
        source_system=source_system or source,
    )
