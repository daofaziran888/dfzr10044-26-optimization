"""入湖批次幂等重传、旧摘要兼容、整组补传和快照读取的回归测试。"""
from __future__ import annotations

from app.lake_store import LakeStore, legacy_batch_checksum
from app.services.ingest_batch import IngestBatchService


def make_service() -> IngestBatchService:
    return IngestBatchService(LakeStore())


def test_reordered_same_source_is_one_logical_batch_but_source_b_stays_separate():
    svc = make_service()
    table = "payment_window"
    row_a = {"trade": "A-1", "amount": 12, "currency": "CNY"}
    row_a_reordered = {"currency": "CNY", "amount": 12, "trade": "A-1"}
    row_b = {"trade": "A-1", "amount": 12, "currency": "CNY"}

    first = svc.ingest_batch(table, "A", [row_a], "w-start", "w-end", source_credential="cred-A")
    retry = svc.ingest_batch(table, "A", [row_a_reordered], "w-start", "w-end", source_credential="cred-A-retry")
    second_source = svc.ingest_batch(table, "B", [row_b], "w-start", "w-end", source_credential="cred-B")

    assert first["logical_batch_id"] == retry["logical_batch_id"]
    assert retry["replayed"] is True
    assert first["canonical_checksum"] == retry["canonical_checksum"]
    assert second_source["logical_batch_id"] != first["logical_batch_id"]

    svc.confirm_ingestion(first["batch_id"], "snap-A")
    svc.confirm_ingestion(second_source["batch_id"], "snap-B")
    rows = svc.read_ingested_rows(table)

    assert len(rows) == 2
    assert sorted((r["source_system"], r["amount"]) for r in rows) == [("A", 12), ("B", 12)]
    a_rows = [r for r in rows if r["source_system"] == "A"]
    assert len(a_rows) == 1
    assert a_rows[0]["source_credential"] == "cred-A"
    assert a_rows[0]["accepted_range"]["indexes"] == [0]


def test_later_18_is_distinct_input_and_both_attempt_histories_exist():
    svc = make_service()
    table = "later_amount"
    row_12 = {"id": "A1", "amount": 12}
    row_12_reordered = {"amount": 12, "id": "A1"}
    row_18 = {"id": "A2", "amount": 18}

    first = svc.ingest_batch(table, "A", [row_12], "w", "z")
    svc.ingest_batch(table, "A", [row_12_reordered], "w", "z")
    later = svc.ingest_batch(table, "A", [row_18], "w", "z")

    assert first["logical_batch_id"] != later["logical_batch_id"]
    detail_12 = svc.get_batch_detail(first["batch_id"])
    detail_18 = svc.get_batch_detail(later["batch_id"])
    assert detail_12["attempt_count"] == 2
    assert detail_18["attempt_count"] == 1

    svc.confirm_ingestion(first["batch_id"], "snap-12")
    svc.confirm_ingestion(later["batch_id"], "snap-18")
    rows = svc.read_ingested_rows(table)
    assert [(r["source_system"], r["amount"]) for r in rows] == [("A", 12), ("A", 18)]


def test_legacy_checksum_verifies_original_and_reconciles_reordered_payload():
    svc = make_service()
    original = [{"b": 2, "a": 1, "nested": {"z": 1, "y": 2}}]
    reordered = [{"nested": {"y": 2, "z": 1}, "a": 1, "b": 2}]
    old_digest = legacy_batch_checksum(original)

    plain = svc.verify_checksum(old_digest, original)
    assert plain["integrity_ok"] is True

    received = svc.ingest_batch("legacy_table", "A", original, "w", "z")
    result = svc.verify_checksum(
        old_digest,
        reordered,
        lake_table="legacy_table",
        source_system="A",
        window_start="w",
        window_end="z",
    )

    assert result["matched"] is True
    assert result["matched_logical_batch_id"] == received["logical_batch_id"]
    assert result["original_input"] == original
    assert result["old_checksum_verifies_original"] is True
    assert result["integrity_ok"] is True
    assert result["strict_integrity_ok"] is False
    assert result["reconciliation_needed"] is True
    assert result["reconciliation"]["canonical_content_matches"] is True
    assert result["reconciliation"]["legacy_digest_matches_payload"] is False
    assert result["reconciliation"]["old_checksum_verifies_original_input"] is True


def test_group_retransmit_can_split_accept_new_item_with_group_index_scope():
    svc = make_service()
    table = "group_partial"
    row_12 = {"id": "A1", "amount": 12}
    row_12_reordered = {"amount": 12, "id": "A1"}
    row_18 = {"id": "A2", "amount": 18}

    first = svc.ingest_batch(table, "A", [row_12], "w", "z")
    group = svc.retransmit_group(
        table, "A", [row_12_reordered, row_18], "w", "z", allow_partial=True,
    )

    assert group["accepted_item_indexes"] == [1]
    assert group["handoff_item_indexes"] == [0]
    assert group["accepted_ranges"] == [{"start": 1, "end": 2, "indexes": [1]}]
    new_batch = group["accepted_items"][0]["logical_batch_id"]
    svc.confirm_ingestion(new_batch, "snap-18-group")
    rows = svc.read_ingested_rows(table, "snap-18-group")
    assert len(rows) == 1
    assert rows[0]["amount"] == 18
    assert rows[0]["accepted_range"]["indexes"] == [1]

    # 接手旧的 12 元逻辑批次并确认，不会产生第二份 A/12。
    svc.confirm_ingestion(first["batch_id"], "snap-12")
    all_rows = svc.read_ingested_rows(table)
    assert len([r for r in all_rows if r["amount"] == 12]) == 1


def test_atomic_group_retry_reports_pending_new_part_before_confirmation():
    svc = make_service()
    table = "group_atomic"
    row_12 = {"id": "A1", "amount": 12}
    row_18 = {"id": "A2", "amount": 18}

    svc.ingest_batch(table, "A", [row_12], "w", "z")
    result = svc.retransmit_group(
        table,
        "A",
        [{"amount": 12, "id": "A1"}, row_18],
        "w",
        "z",
        wait_for_previous=True,
    )

    assert result["must_wait_for_previous"] is True
    assert result["waiting_item_indexes"] == [0]
    assert result["pending_item_indexes"] == [1]
    assert result["pending_rows"] == [row_18]
    assert result["pending_part"] == [row_18]
    assert result["pending_part"]["indexes"] == [1]
    assert result["resubmit"]["rows"] == [row_18]
