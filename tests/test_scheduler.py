"""
Unit tests for WATER Scheduler — covers both the policy interface and
the Scheduler core logic.

Run with: pytest tests/test_scheduler.py -v
"""
import tempfile
from pathlib import Path

import pytest

from water.registry.node_registry import Node, NodeRegistry, NodeStatus
from water.scheduler.scheduler import (
    DefaultPolicy,
    LatencyOptimizedPolicy,
    PrivacyFirstPolicy,
    Scheduler,
    SchedulerError,
    available_policies,
    get_policy,
)
from water.schema.workflow import (
    ComputeProfile,
    LatencyProfile,
    PrivacyLevel,
    WorkflowIntent,
    WorkflowStep,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def registry():
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "sched_test.db"
        r = NodeRegistry(db_path=db)
        r.register(Node(
            id="edge-local-01", hostname="10.0.1.1", node_type="edge",
            labels="zone=local,type=edge,role=pacs", cpu_cores=4, memory_gb=8.0, gpu_count=0,
        ))
        r.register(Node(
            id="gpu-edge-01", hostname="10.0.1.2", node_type="edge",
            labels="zone=local,type=edge,gpu=true", cpu_cores=16, memory_gb=32.0, gpu_count=2,
        ))
        r.register(Node(
            id="cloud-01", hostname="ec2.aws.com", node_type="cloud",
            labels="zone=cloud,type=cloud", cpu_cores=32, memory_gb=128.0, gpu_count=0,
        ))
        yield r


def make_step(step_id="s", selector=None, image="test:latest", gpu_env=False):
    env = {"USE_GPU": "true"} if gpu_env else {}
    return WorkflowStep(id=step_id, name=step_id, image=image,
                        node_selector=selector, env=env)


def make_intent(privacy=PrivacyLevel.PUBLIC, compute=ComputeProfile.CPU_LIGHT,
                latency=LatencyProfile.BATCH) -> WorkflowIntent:
    return WorkflowIntent(privacy=privacy, compute=compute, latency=latency)


# ---------------------------------------------------------------------------
# Policy registry
# ---------------------------------------------------------------------------

class TestPolicyRegistry:
    def test_available_policies_returns_all(self):
        policies = available_policies()
        assert "privacy-first" in policies
        assert "latency-optimized" in policies
        assert "default" in policies

    def test_get_policy_known(self):
        p = get_policy("privacy-first")
        assert isinstance(p, PrivacyFirstPolicy)

    def test_get_policy_unknown_raises(self):
        with pytest.raises(KeyError, match="Unknown policy"):
            get_policy("does-not-exist")


# ---------------------------------------------------------------------------
# PrivacyFirstPolicy
# ---------------------------------------------------------------------------

class TestPrivacyFirstPolicy:
    def setup_method(self):
        self.policy = PrivacyFirstPolicy()

    def _nodes(self, registry):
        return registry.list_available()

    def test_strict_local_removes_cloud_nodes(self, registry):
        intent = make_intent(privacy=PrivacyLevel.STRICT_LOCAL)
        step = make_step()
        candidates = self.policy.filter(self._nodes(registry), step, intent)
        assert all(n.has_label("zone", "local") for n in candidates)
        assert not any(n.id == "cloud-01" for n in candidates)

    def test_strict_local_returns_empty_when_no_local_nodes(self, registry):
        # Mark local nodes offline
        registry.update_status("edge-local-01", NodeStatus.OFFLINE)
        registry.update_status("gpu-edge-01", NodeStatus.OFFLINE)
        intent = make_intent(privacy=PrivacyLevel.STRICT_LOCAL)
        candidates = self.policy.filter(registry.list_available(), make_step(), intent)
        assert candidates == []

    def test_edge_only_falls_back_to_all_if_no_edge(self, registry):
        registry.update_status("edge-local-01", NodeStatus.OFFLINE)
        registry.update_status("gpu-edge-01", NodeStatus.OFFLINE)
        intent = make_intent(privacy=PrivacyLevel.EDGE_ONLY)
        candidates = self.policy.filter(registry.list_available(), make_step(), intent)
        # Cloud is the only available node — should be returned as fallback
        assert any(n.id == "cloud-01" for n in candidates)

    def test_public_no_zone_filter(self, registry):
        intent = make_intent(privacy=PrivacyLevel.PUBLIC)
        candidates = self.policy.filter(self._nodes(registry), make_step(), intent)
        assert len(candidates) == 3  # all nodes pass


# ---------------------------------------------------------------------------
# LatencyOptimizedPolicy
# ---------------------------------------------------------------------------

class TestLatencyOptimizedPolicy:
    def setup_method(self):
        self.policy = LatencyOptimizedPolicy()

    def test_prefers_edge_node_over_cloud(self, registry):
        intent = make_intent(privacy=PrivacyLevel.PUBLIC, latency=LatencyProfile.INTERACTIVE)
        step = make_step()
        nodes = registry.list_available()
        scores = {n.id: self.policy.score(n, step, intent) for n in nodes}
        # Both edge nodes should outrank cloud
        assert scores["cloud-01"] < scores["edge-local-01"]
        assert scores["cloud-01"] < scores["gpu-edge-01"]

    def test_inherits_privacy_filtering(self, registry):
        intent = make_intent(privacy=PrivacyLevel.STRICT_LOCAL)
        candidates = self.policy.filter(registry.list_available(), make_step(), intent)
        assert all(n.has_label("zone", "local") for n in candidates)


# ---------------------------------------------------------------------------
# DefaultPolicy
# ---------------------------------------------------------------------------

class TestDefaultPolicy:
    def setup_method(self):
        self.policy = DefaultPolicy()

    def test_gpu_required_filters_to_gpu_nodes(self, registry):
        intent = make_intent(compute=ComputeProfile.GPU_REQUIRED)
        candidates = self.policy.filter(registry.list_available(), make_step(), intent)
        assert all(n.gpu_count > 0 for n in candidates)
        assert candidates[0].id == "gpu-edge-01"

    def test_cpu_heavy_filters_to_capable_nodes(self, registry):
        intent = make_intent(compute=ComputeProfile.CPU_HEAVY)
        candidates = self.policy.filter(registry.list_available(), make_step(), intent)
        # Only gpu-edge-01 (16 cores, 32GB) and cloud-01 (32 cores, 128GB) qualify
        ids = {n.id for n in candidates}
        assert "edge-local-01" not in ids

    def test_gpu_image_boosts_score_on_gpu_node(self, registry):
        intent = make_intent()
        step = make_step(image="myorg/gpu-inference:latest")
        nodes = registry.list_available()
        scores = {n.id: self.policy.score(n, step, intent) for n in nodes}
        assert scores["gpu-edge-01"] > scores["edge-local-01"]


# ---------------------------------------------------------------------------
# Scheduler core
# ---------------------------------------------------------------------------

class TestScheduler:
    def test_default_policy_picks_best_node(self, registry):
        scheduler = Scheduler(registry)
        node = scheduler.assign(make_step(), make_intent())
        # cloud-01 (32 cpu, 128GB) scores highest with default policy
        assert node is not None

    def test_node_selector_overrides_policy(self, registry):
        scheduler = Scheduler(registry, policy=PrivacyFirstPolicy())
        step = make_step(selector="role=pacs")
        # node_selector pins to pacs node regardless of policy
        node = scheduler.assign(step, make_intent(privacy=PrivacyLevel.PUBLIC))
        assert node.id == "edge-local-01"

    def test_assign_by_id_selector(self, registry):
        scheduler = Scheduler(registry)
        step = make_step(selector="cloud-01")
        node = scheduler.assign(step, make_intent())
        assert node.id == "cloud-01"

    def test_strict_local_rejects_cloud(self, registry):
        scheduler = Scheduler(registry, policy=PrivacyFirstPolicy())
        intent = make_intent(privacy=PrivacyLevel.STRICT_LOCAL)
        # strict-local: only zone=local nodes eligible
        node = scheduler.assign(make_step(), intent)
        assert node.has_label("zone", "local")

    def test_no_available_nodes_raises(self, registry):
        for n in registry.list_all():
            registry.update_status(n.id, NodeStatus.OFFLINE)
        scheduler = Scheduler(registry)
        with pytest.raises(SchedulerError, match="No available nodes"):
            scheduler.assign(make_step(), make_intent())

    def test_unmatched_selector_raises(self, registry):
        scheduler = Scheduler(registry)
        with pytest.raises(SchedulerError, match="matched no available nodes"):
            scheduler.assign(make_step(selector="role=nonexistent"), make_intent())

    def test_offline_node_excluded(self, registry):
        registry.update_status("gpu-edge-01", NodeStatus.OFFLINE)
        scheduler = Scheduler(registry)
        node = scheduler.assign(make_step(gpu_env=True), make_intent())
        assert node.id != "gpu-edge-01"

    def test_latency_policy_via_get_policy(self, registry):
        scheduler = Scheduler(registry, policy=get_policy("latency-optimized"))
        intent = make_intent(privacy=PrivacyLevel.PUBLIC, latency=LatencyProfile.INTERACTIVE)
        node = scheduler.assign(make_step(), intent)
        # Latency policy prefers edge nodes
        assert node.has_label("type", "edge")

    def test_privacy_policy_via_get_policy(self, registry):
        scheduler = Scheduler(registry, policy=get_policy("privacy-first"))
        intent = make_intent(privacy=PrivacyLevel.STRICT_LOCAL)
        node = scheduler.assign(make_step(), intent)
        assert node.has_label("zone", "local")
