"""
Unit tests for WATER AuditTrail (tamper-evident execution receipts).
Run with: pytest tests/test_audit.py -v
"""
import tempfile
from pathlib import Path

import pytest

from water.engine.audit import AuditTrail, ExecutionReceipt


@pytest.fixture
def audit():
    with tempfile.TemporaryDirectory() as tmp:
        yield AuditTrail(db_path=Path(tmp) / "test_audit.sqlite")


class TestExecutionReceipt:
    def test_hash_deterministic(self):
        r1 = ExecutionReceipt(
            receipt_id="r1", run_id="run-1", step_id="step-a",
            node_id="edge-01", image="myorg/img:1.0", exit_code=0,
            stdout_snippet="ok", timestamp=1000.0, prev_hash="GENESIS",
        )
        r2 = ExecutionReceipt(
            receipt_id="r1", run_id="run-1", step_id="step-a",
            node_id="edge-01", image="myorg/img:1.0", exit_code=0,
            stdout_snippet="ok", timestamp=1000.0, prev_hash="GENESIS",
        )
        assert r1.receipt_hash == r2.receipt_hash

    def test_different_exit_code_changes_hash(self):
        base = dict(receipt_id="r1", run_id="run-1", step_id="step-a",
                    node_id="edge-01", image="myorg/img:1.0",
                    stdout_snippet="ok", timestamp=1000.0, prev_hash="GENESIS")
        r_ok  = ExecutionReceipt(**base, exit_code=0)
        r_err = ExecutionReceipt(**base, exit_code=1)
        assert r_ok.receipt_hash != r_err.receipt_hash


class TestAuditTrail:
    def test_record_returns_receipt(self, audit):
        r = audit.record(
            step_id="preprocess", run_id="run-1",
            node_id="edge-01", image="myorg/pre:1.0",
            exit_code=0, stdout_snippet="done",
        )
        assert isinstance(r, ExecutionReceipt)
        assert r.step_id == "preprocess"
        assert r.prev_hash == "GENESIS"

    def test_chain_links_receipts(self, audit):
        r1 = audit.record("step-a", "run-1", "n1", "img:1", 0)
        r2 = audit.record("step-b", "run-1", "n1", "img:1", 0)
        assert r2.prev_hash == r1.receipt_hash

    def test_verify_chain_intact(self, audit):
        audit.record("step-a", "run-1", "n1", "img:1", 0)
        audit.record("step-b", "run-1", "n1", "img:1", 0)
        audit.record("step-c", "run-1", "n1", "img:1", 0)
        ok, msg = audit.verify_chain()
        assert ok, msg

    def test_verify_chain_empty(self, audit):
        ok, msg = audit.verify_chain()
        assert ok
        assert "empty" in msg

    def test_tamper_detected(self, audit):
        audit.record("step-a", "run-1", "n1", "img:1", 0)
        audit.record("step-b", "run-1", "n1", "img:1", 0)

        # Manually corrupt the first receipt's hash in the DB
        audit._conn.execute(
            "UPDATE receipts SET receipt_hash='tampered' WHERE step_id='step-a'"
        )
        audit._conn.commit()

        ok, msg = audit.verify_chain()
        assert not ok
        assert "tampered" in msg or "mismatch" in msg or "broken" in msg

    def test_list_receipts_by_run(self, audit):
        audit.record("step-a", "run-1", "n1", "img:1", 0)
        audit.record("step-b", "run-1", "n1", "img:1", 0)
        audit.record("step-x", "run-2", "n1", "img:1", 0)

        receipts = audit.list_receipts(run_id="run-1")
        assert len(receipts) == 2
        assert all(r.run_id == "run-1" for r in receipts)

    def test_list_all_receipts(self, audit):
        audit.record("step-a", "run-1", "n1", "img:1", 0)
        audit.record("step-b", "run-2", "n1", "img:1", 1)
        receipts = audit.list_receipts()
        assert len(receipts) == 2
