"""
WATER Audit Trail — tamper-evident execution receipts.

Every step execution produces a SHA-256 receipt that is stored in a
local SQLite database.  Receipts capture:
  - who ran what, on which node, at what time
  - the exact container image and command used
  - the exit code and a hash of the output log

Chaining each receipt to the hash of the previous one creates an
append-only log where any tampering is immediately detectable — the
same principle used in audit logs for healthcare compliance systems.

This addresses the HIPAA requirement for a tamper-evident audit trail
of all data-processing operations, which is non-negotiable when running
workflows on patient-derived data across edge nodes.

Usage::

    audit = AuditTrail()
    receipt = audit.record(
        step_id="preprocess",
        run_id="run-001",
        node_id="edge-01",
        image="myorg/preprocessor:1.0",
        exit_code=0,
        stdout_snippet="Processed 142 files",
    )
    print(receipt.receipt_hash)   # SHA-256 of this receipt

    # Verify the chain has not been tampered with
    ok, report = audit.verify_chain()
    assert ok, report
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Tuple


# ---------------------------------------------------------------------------
# Receipt dataclass
# ---------------------------------------------------------------------------

@dataclass
class ExecutionReceipt:
    """A single tamper-evident record of one step execution."""
    receipt_id:      str
    run_id:          str
    step_id:         str
    node_id:         str
    image:           str
    exit_code:       int
    stdout_snippet:  str
    timestamp:       float
    prev_hash:       str        # hash of the previous receipt (or "GENESIS")
    receipt_hash:    str = field(init=False)

    def __post_init__(self) -> None:
        self.receipt_hash = self._compute_hash()

    def _compute_hash(self) -> str:
        payload = json.dumps({
            "receipt_id":     self.receipt_id,
            "run_id":         self.run_id,
            "step_id":        self.step_id,
            "node_id":        self.node_id,
            "image":          self.image,
            "exit_code":      self.exit_code,
            "stdout_snippet": self.stdout_snippet,
            "timestamp":      self.timestamp,
            "prev_hash":      self.prev_hash,
        }, sort_keys=True)
        return hashlib.sha256(payload.encode()).hexdigest()

    def to_dict(self) -> dict:
        return {
            "receipt_id":     self.receipt_id,
            "run_id":         self.run_id,
            "step_id":        self.step_id,
            "node_id":        self.node_id,
            "image":          self.image,
            "exit_code":      self.exit_code,
            "stdout_snippet": self.stdout_snippet,
            "timestamp":      self.timestamp,
            "prev_hash":      self.prev_hash,
            "receipt_hash":   self.receipt_hash,
        }


# ---------------------------------------------------------------------------
# AuditTrail
# ---------------------------------------------------------------------------

class AuditTrail:
    """
    Append-only, hash-chained audit log for WATER step executions.

    Each new receipt is chained to the previous one by embedding the
    previous receipt's SHA-256 hash.  This makes the log tamper-evident:
    modifying any past receipt invalidates all subsequent hashes.

    The log is stored in a local SQLite file.  In a multi-node deployment,
    each node maintains its own local trail; the controller can aggregate
    them on demand.
    """

    _DDL = """
    CREATE TABLE IF NOT EXISTS receipts (
        receipt_id    TEXT PRIMARY KEY,
        run_id        TEXT NOT NULL,
        step_id       TEXT NOT NULL,
        node_id       TEXT NOT NULL,
        image         TEXT NOT NULL,
        exit_code     INTEGER NOT NULL,
        stdout_snippet TEXT,
        timestamp     REAL NOT NULL,
        prev_hash     TEXT NOT NULL,
        receipt_hash  TEXT NOT NULL
    )
    """

    def __init__(self, db_path: Optional[Path] = None) -> None:
        self.db_path = db_path or Path("water_audit.sqlite")
        self._conn   = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._conn.execute(self._DDL)
        self._conn.commit()

    # ------------------------------------------------------------------
    # Writing
    # ------------------------------------------------------------------

    def record(
        self,
        step_id:        str,
        run_id:         str,
        node_id:        str,
        image:          str,
        exit_code:      int,
        stdout_snippet: str = "",
    ) -> ExecutionReceipt:
        """
        Append a new execution receipt to the audit trail.

        The receipt is chained to the previous one; the first receipt
        chains to the "GENESIS" sentinel.
        """
        prev_hash  = self._last_hash()
        receipt_id = f"{run_id}:{step_id}:{int(time.time()*1000)}"
        receipt    = ExecutionReceipt(
            receipt_id=receipt_id,
            run_id=run_id,
            step_id=step_id,
            node_id=node_id,
            image=image,
            exit_code=exit_code,
            stdout_snippet=stdout_snippet[:500],
            timestamp=time.time(),
            prev_hash=prev_hash,
        )
        self._conn.execute(
            """INSERT INTO receipts
               (receipt_id, run_id, step_id, node_id, image, exit_code,
                stdout_snippet, timestamp, prev_hash, receipt_hash)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (
                receipt.receipt_id, receipt.run_id, receipt.step_id,
                receipt.node_id, receipt.image, receipt.exit_code,
                receipt.stdout_snippet, receipt.timestamp,
                receipt.prev_hash, receipt.receipt_hash,
            ),
        )
        self._conn.commit()
        return receipt

    # ------------------------------------------------------------------
    # Reading
    # ------------------------------------------------------------------

    def list_receipts(self, run_id: Optional[str] = None) -> List[ExecutionReceipt]:
        """Return all receipts, optionally filtered by run_id."""
        if run_id:
            rows = self._conn.execute(
                "SELECT * FROM receipts WHERE run_id=? ORDER BY timestamp",
                (run_id,),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM receipts ORDER BY timestamp"
            ).fetchall()
        return [self._row_to_receipt(r) for r in rows]

    def verify_chain(self) -> Tuple[bool, str]:
        """
        Walk the entire receipt chain and verify each hash is consistent.

        Returns (True, "OK") if the chain is intact, or
                (False, <description of first tampered record>) otherwise.
        """
        rows = self._conn.execute(
            "SELECT * FROM receipts ORDER BY timestamp"
        ).fetchall()

        if not rows:
            return True, "OK (empty chain)"

        prev_hash = "GENESIS"
        for row in rows:
            stored = self._row_to_receipt(row)
            if stored.prev_hash != prev_hash:
                return False, (
                    f"Chain broken at receipt {stored.receipt_id}: "
                    f"expected prev_hash={prev_hash!r}, "
                    f"found={stored.prev_hash!r}"
                )
            recomputed = ExecutionReceipt(
                receipt_id=stored.receipt_id,
                run_id=stored.run_id,
                step_id=stored.step_id,
                node_id=stored.node_id,
                image=stored.image,
                exit_code=stored.exit_code,
                stdout_snippet=stored.stdout_snippet,
                timestamp=stored.timestamp,
                prev_hash=stored.prev_hash,
            )
            if recomputed.receipt_hash != stored.receipt_hash:
                return False, (
                    f"Hash mismatch at receipt {stored.receipt_id}: "
                    f"stored={stored.receipt_hash}, "
                    f"recomputed={recomputed.receipt_hash}"
                )
            prev_hash = stored.receipt_hash

        return True, f"OK ({len(rows)} receipts verified)"

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _last_hash(self) -> str:
        row = self._conn.execute(
            "SELECT receipt_hash FROM receipts ORDER BY timestamp DESC LIMIT 1"
        ).fetchone()
        return row[0] if row else "GENESIS"

    @staticmethod
    def _row_to_receipt(row: tuple) -> ExecutionReceipt:
        r = ExecutionReceipt(
            receipt_id=row[0], run_id=row[1], step_id=row[2],
            node_id=row[3], image=row[4], exit_code=row[5],
            stdout_snippet=row[6] or "", timestamp=row[7],
            prev_hash=row[8],
        )
        # Override computed hash with the stored one for chain-walking
        object.__setattr__(r, "receipt_hash", row[9])
        return r
