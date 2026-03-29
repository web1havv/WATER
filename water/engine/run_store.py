"""
WATER RunStore — SQLite-backed persistent workflow run storage.

Unlike the in-memory dict in app.py, the RunStore survives API restarts.
Each WorkflowRun and its StepResults are serialized to JSON and stored
in a single SQLite table. This is the Week 6 improvement over the MVP.

Design: store run metadata in columns for fast querying (status, name, time),
        store full StepResults as JSON blob for flexibility.
"""
from __future__ import annotations

import json
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path
from typing import Generator, List, Optional

from water.engine.execution_engine import StepResult, WorkflowRun
from water.schema.workflow import StepStatus

DB_PATH = Path.home() / ".water" / "runs.db"


class RunStore:
    """
    Persistent SQLite store for WorkflowRun objects.

    Schema:
      runs(run_id TEXT PK, workflow_name TEXT, status TEXT,
           started_at REAL, finished_at REAL, results_json TEXT)
    """

    def __init__(self, db_path: Path = DB_PATH) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    @contextmanager
    def _conn(self) -> Generator[sqlite3.Connection, None, None]:
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_db(self) -> None:
        with self._conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS runs (
                    run_id       TEXT PRIMARY KEY,
                    workflow_name TEXT NOT NULL,
                    status       TEXT NOT NULL,
                    started_at   REAL NOT NULL,
                    finished_at  REAL,
                    results_json TEXT NOT NULL DEFAULT '[]'
                )
            """)

    def save(self, run_id: str, run: WorkflowRun) -> None:
        """Persist or update a WorkflowRun."""
        results_json = json.dumps([
            {
                "step_id": r.step_id,
                "node_id": r.node_id,
                "status": r.status.value,
                "exit_code": r.exit_code,
                "duration_seconds": r.duration_seconds,
                "error": r.error,
            }
            for r in run.results
        ])
        finished_at = time.time() if run.status in (
            StepStatus.COMPLETED, StepStatus.FAILED
        ) else None

        with self._conn() as conn:
            conn.execute("""
                INSERT INTO runs (run_id, workflow_name, status, started_at, finished_at, results_json)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    status=excluded.status,
                    finished_at=excluded.finished_at,
                    results_json=excluded.results_json
            """, (
                run_id,
                run.workflow_name,
                run.status.value,
                run.started_at,
                finished_at,
                results_json,
            ))

    def get(self, run_id: str) -> Optional[WorkflowRun]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if not row:
                return None
            return self._row_to_run(row)

    def list_all(self, limit: int = 50) -> List[tuple[str, WorkflowRun]]:
        """Return list of (run_id, run) sorted by most recent first."""
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM runs ORDER BY started_at DESC LIMIT ?", (limit,)
            ).fetchall()
            return [(row["run_id"], self._row_to_run(row)) for row in rows]

    def list_by_status(self, status: StepStatus) -> List[tuple[str, WorkflowRun]]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM runs WHERE status = ? ORDER BY started_at DESC",
                (status.value,)
            ).fetchall()
            return [(row["run_id"], self._row_to_run(row)) for row in rows]

    def _row_to_run(self, row: sqlite3.Row) -> WorkflowRun:
        results_data = json.loads(row["results_json"])
        results = [
            StepResult(
                step_id=r["step_id"],
                node_id=r["node_id"],
                status=StepStatus(r["status"]),
                exit_code=r.get("exit_code"),
                duration_seconds=r.get("duration_seconds", 0.0),
                error=r.get("error"),
            )
            for r in results_data
        ]
        run = WorkflowRun(
            workflow_name=row["workflow_name"],
            started_at=row["started_at"],
        )
        run.status = StepStatus(row["status"])
        run.results = results
        return run


# ── Self-test ─────────────────────────────────────────────────────────
def _test():
    import tempfile
    from water.schema.workflow import StepStatus

    with tempfile.TemporaryDirectory() as tmp:
        store = RunStore(db_path=Path(tmp) / "test_runs.db")

        run = WorkflowRun(workflow_name="test-pipeline")
        run.results = [
            StepResult("cold-extraction", "edge-01", StepStatus.COMPLETED, 0, 12.3),
            StepResult("png-extraction", "gpu-01", StepStatus.COMPLETED, 0, 45.6),
        ]
        run.status = StepStatus.COMPLETED

        store.save("run-abc123", run)
        fetched = store.get("run-abc123")

        assert fetched is not None
        assert fetched.workflow_name == "test-pipeline"
        assert fetched.status == StepStatus.COMPLETED
        assert len(fetched.results) == 2
        assert fetched.results[0].step_id == "cold-extraction"
        assert fetched.results[1].duration_seconds == 45.6

        all_runs = store.list_all()
        assert len(all_runs) == 1

        print("RunStore self-test: PASS ✓")
        print(f"  Stored and retrieved run 'run-abc123'")
        print(f"  Steps: {[r.step_id for r in fetched.results]}")
        print(f"  Status: {fetched.status}")


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    _test()
