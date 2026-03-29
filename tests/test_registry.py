"""
Unit tests for NodeRegistry (SQLite-backed).
Run with: pytest tests/test_registry.py -v
"""
import tempfile
import time
from pathlib import Path

import pytest

from water.registry.node_registry import Node, NodeRegistry, NodeStatus


@pytest.fixture
def registry():
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "test_registry.db"
        yield NodeRegistry(db_path=db)


def make_node(node_id="n1", node_type="edge", labels="", gpu=0):
    return Node(
        id=node_id,
        hostname=f"{node_id}.example.com",
        node_type=node_type,
        labels=labels,
        cpu_cores=4,
        memory_gb=8.0,
        gpu_count=gpu,
    )


class TestNodeRegistry:
    def test_register_and_get(self, registry):
        n = make_node("edge-01")
        registry.register(n)
        fetched = registry.get("edge-01")
        assert fetched is not None
        assert fetched.hostname == "edge-01.example.com"

    def test_get_missing_returns_none(self, registry):
        assert registry.get("no-such-node") is None

    def test_list_all_empty(self, registry):
        assert registry.list_all() == []

    def test_list_all_returns_registered(self, registry):
        registry.register(make_node("n1"))
        registry.register(make_node("n2"))
        all_nodes = registry.list_all()
        ids = {n.id for n in all_nodes}
        assert ids == {"n1", "n2"}

    def test_list_available_filters_offline(self, registry):
        registry.register(make_node("online-node"))
        registry.register(make_node("offline-node"))
        registry.update_status("offline-node", NodeStatus.OFFLINE)
        available = registry.list_available()
        assert len(available) == 1
        assert available[0].id == "online-node"

    def test_update_status(self, registry):
        registry.register(make_node("n"))
        registry.update_status("n", NodeStatus.MAINTENANCE)
        n = registry.get("n")
        assert n.status == NodeStatus.MAINTENANCE

    def test_heartbeat_updates_timestamp(self, registry):
        registry.register(make_node("n"))
        before = registry.get("n").last_heartbeat
        time.sleep(0.05)
        registry.heartbeat("n")
        after = registry.get("n").last_heartbeat
        assert after > before

    def test_remove_node(self, registry):
        registry.register(make_node("to-delete"))
        registry.remove("to-delete")
        assert registry.get("to-delete") is None

    def test_register_overwrites_on_conflict(self, registry):
        registry.register(make_node("n", node_type="edge"))
        registry.register(make_node("n", node_type="cloud"))
        n = registry.get("n")
        assert n.node_type == "cloud"

    def test_find_by_label(self, registry):
        registry.register(make_node("gpu-node", labels="role=gpu-worker,region=alaska"))
        registry.register(make_node("pacs-node", labels="role=pacs-gateway"))
        gpu_nodes = registry.find_by_label("role", "gpu-worker")
        assert len(gpu_nodes) == 1
        assert gpu_nodes[0].id == "gpu-node"

    def test_gpu_count_stored(self, registry):
        registry.register(make_node("gpu-01", gpu=4))
        n = registry.get("gpu-01")
        assert n.gpu_count == 4
