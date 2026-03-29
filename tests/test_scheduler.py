"""
Unit tests for WATER Scheduler.
Run with: pytest tests/test_scheduler.py -v
"""
import tempfile
from pathlib import Path

import pytest

from water.registry.node_registry import Node, NodeRegistry, NodeStatus
from water.scheduler.scheduler import Scheduler, SchedulerError
from water.schema.workflow import WorkflowStep


@pytest.fixture
def registry():
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "sched_test.db"
        r = NodeRegistry(db_path=db)
        r.register(Node(id="pacs-01", hostname="10.0.1.1", node_type="edge",
                        labels="role=pacs-gateway", cpu_cores=4, memory_gb=8.0, gpu_count=0))
        r.register(Node(id="gpu-01", hostname="10.0.1.2", node_type="edge",
                        labels="role=gpu-worker", cpu_cores=16, memory_gb=32.0, gpu_count=2))
        r.register(Node(id="cloud-01", hostname="ec2.amazonaws.com", node_type="cloud",
                        labels="role=cloud-indexer", cpu_cores=8, memory_gb=16.0, gpu_count=0))
        yield r


@pytest.fixture
def scheduler(registry):
    return Scheduler(registry)


def make_step(step_id="s", selector=None, image="test:latest", gpu_env=False):
    env = {"USE_GPU": "true"} if gpu_env else {}
    return WorkflowStep(
        id=step_id,
        name=step_id,
        image=image,
        node_selector=selector,
        env=env,
    )


class TestScheduler:
    def test_assign_no_selector_returns_best_node(self, scheduler):
        step = make_step()
        node = scheduler.assign(step)
        assert node is not None
        # Highest scoring: gpu-01 (16 CPU, 32GB, 2 GPU)
        assert node.id == "gpu-01"

    def test_assign_label_selector(self, scheduler):
        step = make_step(selector="role=pacs-gateway")
        node = scheduler.assign(step)
        assert node.id == "pacs-01"

    def test_assign_id_selector(self, scheduler):
        step = make_step(selector="cloud-01")
        node = scheduler.assign(step)
        assert node.id == "cloud-01"

    def test_assign_gpu_preference(self, scheduler):
        # No selector — should prefer GPU node
        step = make_step(gpu_env=True)
        node = scheduler.assign(step)
        assert node.gpu_count > 0

    def test_assign_gpu_image_name(self, scheduler):
        step = make_step(image="niffler/gpu-png-extraction:latest")
        node = scheduler.assign(step)
        assert node.gpu_count > 0

    def test_assign_fails_no_nodes_available(self, registry, scheduler):
        # Mark all nodes offline
        for n in registry.list_all():
            registry.update_status(n.id, NodeStatus.OFFLINE)
        with pytest.raises(SchedulerError, match="No available nodes"):
            scheduler.assign(make_step())

    def test_assign_fails_selector_no_match(self, scheduler):
        step = make_step(selector="role=non-existent")
        with pytest.raises(SchedulerError, match="matched no available nodes"):
            scheduler.assign(step)

    def test_assign_offline_node_excluded(self, registry, scheduler):
        registry.update_status("gpu-01", NodeStatus.OFFLINE)
        step = make_step()
        node = scheduler.assign(step)
        assert node.id != "gpu-01"
