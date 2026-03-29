"""
WATER NodeRegistry — SQLite-backed registry of compute nodes.

Each node represents an edge or cloud resource that WATER can schedule
workflow steps onto. The registry tracks node availability, resource
capacity, and SSH/connection metadata.
"""
from __future__ import annotations

import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass, field, asdict
from enum import Enum
from pathlib import Path
from typing import Generator, List, Optional


DB_PATH = Path.home() / ".water" / "registry.db"


class NodeStatus(str, Enum):
    ONLINE = "online"
    OFFLINE = "offline"
    DEGRADED = "degraded"
    MAINTENANCE = "maintenance"


@dataclass
class Node:
    id: str
    hostname: str
    ssh_user: str = "water"
    ssh_port: int = 22
    ssh_key_path: Optional[str] = None
    node_type: str = "edge"          # edge | cloud | local
    labels: str = ""                 # comma-separated key=value pairs
    cpu_cores: int = 1
    memory_gb: float = 1.0
    gpu_count: int = 0
    data_root: str = "/tmp/water"    # base path for data on this node
    status: str = NodeStatus.ONLINE
    last_heartbeat: float = field(default_factory=time.time)
    registered_at: float = field(default_factory=time.time)

    def has_label(self, key: str, value: str) -> bool:
        for pair in self.labels.split(","):
            if "=" in pair:
                k, v = pair.split("=", 1)
                if k.strip() == key and v.strip() == value:
                    return True
        return False

    def is_available(self) -> bool:
        return self.status == NodeStatus.ONLINE

    def label_dict(self) -> dict:
        result = {}
        for pair in self.labels.split(","):
            if "=" in pair:
                k, v = pair.split("=", 1)
                result[k.strip()] = v.strip()
        return result


class NodeRegistry:
    """
    Persistent SQLite-backed store for WATER compute nodes.

    Usage:
        registry = NodeRegistry()
        registry.register(Node(id="edge-01", hostname="192.168.1.10"))
        nodes = registry.list_available()
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
                CREATE TABLE IF NOT EXISTS nodes (
                    id TEXT PRIMARY KEY,
                    hostname TEXT NOT NULL,
                    ssh_user TEXT DEFAULT 'water',
                    ssh_port INTEGER DEFAULT 22,
                    ssh_key_path TEXT,
                    node_type TEXT DEFAULT 'edge',
                    labels TEXT DEFAULT '',
                    cpu_cores INTEGER DEFAULT 1,
                    memory_gb REAL DEFAULT 1.0,
                    gpu_count INTEGER DEFAULT 0,
                    data_root TEXT DEFAULT '/tmp/water',
                    status TEXT DEFAULT 'online',
                    last_heartbeat REAL,
                    registered_at REAL
                )
            """)

    def register(self, node: Node) -> None:
        """Add or update a node in the registry."""
        with self._conn() as conn:
            conn.execute("""
                INSERT INTO nodes VALUES (
                    :id, :hostname, :ssh_user, :ssh_port, :ssh_key_path,
                    :node_type, :labels, :cpu_cores, :memory_gb, :gpu_count,
                    :data_root, :status, :last_heartbeat, :registered_at
                ) ON CONFLICT(id) DO UPDATE SET
                    hostname=excluded.hostname,
                    ssh_user=excluded.ssh_user,
                    ssh_port=excluded.ssh_port,
                    ssh_key_path=excluded.ssh_key_path,
                    node_type=excluded.node_type,
                    labels=excluded.labels,
                    cpu_cores=excluded.cpu_cores,
                    memory_gb=excluded.memory_gb,
                    gpu_count=excluded.gpu_count,
                    data_root=excluded.data_root,
                    status=excluded.status,
                    last_heartbeat=excluded.last_heartbeat
            """, asdict(node))

    def get(self, node_id: str) -> Optional[Node]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM nodes WHERE id = ?", (node_id,)
            ).fetchone()
            return Node(**dict(row)) if row else None

    def list_all(self) -> List[Node]:
        with self._conn() as conn:
            rows = conn.execute("SELECT * FROM nodes").fetchall()
            return [Node(**dict(r)) for r in rows]

    def list_available(self) -> List[Node]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM nodes WHERE status = ?", (NodeStatus.ONLINE,)
            ).fetchall()
            return [Node(**dict(r)) for r in rows]

    def update_status(self, node_id: str, status: NodeStatus) -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE nodes SET status=?, last_heartbeat=? WHERE id=?",
                (status, time.time(), node_id),
            )

    def heartbeat(self, node_id: str) -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE nodes SET last_heartbeat=? WHERE id=?",
                (time.time(), node_id),
            )

    def remove(self, node_id: str) -> None:
        with self._conn() as conn:
            conn.execute("DELETE FROM nodes WHERE id=?", (node_id,))

    def find_by_label(self, key: str, value: str) -> List[Node]:
        """Find nodes that carry a specific label key=value."""
        return [n for n in self.list_available() if n.has_label(key, value)]
