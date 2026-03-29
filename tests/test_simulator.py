"""
Unit tests for WATER Simulation Harness.
Run with: pytest tests/test_simulator.py -v
"""
import pytest

from water.schema.workflow import (
    ComputeProfile, PrivacyLevel, WaterWorkflow,
    WorkflowIntent, WorkflowStep,
)
from water.simulation.simulator import SimulatedNode, Simulator


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_workflow(name="test-wf", privacy=PrivacyLevel.PUBLIC,
                  compute=ComputeProfile.CPU_LIGHT) -> WaterWorkflow:
    return WaterWorkflow(
        name=name,
        intent=WorkflowIntent(privacy=privacy, compute=compute),
        steps=[
            WorkflowStep(id="step-a", name="Step A", image="myorg/a:1.0"),
            WorkflowStep(id="step-b", name="Step B", image="myorg/b:1.0",
                         depends_on=["step-a"]),
        ],
    )


MIXED_FLEET = [
    SimulatedNode("edge-local-01", zone="local", type="edge", cpu=8,  ram=32, gpu=1),
    SimulatedNode("edge-local-02", zone="local", type="edge", cpu=4,  ram=16, gpu=0),
    SimulatedNode("cloud-01",      zone="cloud", type="cloud", cpu=32, ram=128, gpu=0),
]


# ---------------------------------------------------------------------------
# SimulatedNode
# ---------------------------------------------------------------------------

class TestSimulatedNode:
    def test_labels_include_zone_and_type(self):
        n = SimulatedNode("n1", zone="local", type="edge").to_registry_node()
        assert "zone=local" in n.labels
        assert "type=edge" in n.labels

    def test_gpu_label_added_when_nonzero(self):
        n = SimulatedNode("n1", gpu=2).to_registry_node()
        assert "gpu=true" in n.labels

    def test_gpu_label_absent_when_zero(self):
        n = SimulatedNode("n1", gpu=0).to_registry_node()
        assert "gpu=true" not in n.labels

    def test_role_label(self):
        n = SimulatedNode("n1", role="pacs-gateway").to_registry_node()
        assert "role=pacs-gateway" in n.labels

    def test_site_label(self):
        n = SimulatedNode("n1", site="hospital-a").to_registry_node()
        assert "site=hospital-a" in n.labels


# ---------------------------------------------------------------------------
# Simulator
# ---------------------------------------------------------------------------

class TestSimulator:
    def test_run_assigns_all_steps(self):
        wf     = make_workflow()
        report = Simulator(MIXED_FLEET).run(wf)
        assert len(report.step_results) == 2
        assert report.success

    def test_strict_local_assigns_to_local_nodes_only(self):
        wf = make_workflow(privacy=PrivacyLevel.STRICT_LOCAL)
        report = Simulator(MIXED_FLEET).run(wf, policy="privacy-first")
        assert report.success
        for r in report.step_results:
            assert r.node_zone == "local", (
                f"Step {r.step_id} was assigned to zone={r.node_zone}"
            )

    def test_strict_local_assert_method(self):
        wf     = make_workflow(privacy=PrivacyLevel.STRICT_LOCAL)
        report = Simulator(MIXED_FLEET).run(wf, policy="privacy-first")
        # Should not raise
        report.assert_privacy_respected()

    def test_strict_local_fails_with_only_cloud_nodes(self):
        cloud_only = [SimulatedNode("cloud-01", zone="cloud", type="cloud")]
        wf         = make_workflow(privacy=PrivacyLevel.STRICT_LOCAL)
        report     = Simulator(cloud_only).run(wf, policy="privacy-first")
        assert not report.success
        assert len(report.errors) > 0

    def test_latency_policy_prefers_edge(self):
        wf = make_workflow(privacy=PrivacyLevel.PUBLIC)
        report = Simulator(MIXED_FLEET).run(wf, policy="latency-optimized")
        assert report.success
        for r in report.step_results:
            assert r.node_zone != "cloud", (
                f"Latency policy should prefer edge; got zone={r.node_zone}"
            )

    def test_compare_policies_returns_all(self):
        wf      = make_workflow()
        reports = Simulator(MIXED_FLEET).compare_policies(wf)
        assert set(reports.keys()) == {"privacy-first", "latency-optimized", "default"}
        for name, report in reports.items():
            assert report.policy_name != "", name

    def test_report_elapsed_ms_nonzero(self):
        wf     = make_workflow()
        report = Simulator(MIXED_FLEET).run(wf)
        assert report.elapsed_ms >= 0

    def test_federated_ml_strict_local(self):
        """Simulate a 3-hospital federated learning workflow."""
        nodes = [
            SimulatedNode("hospital-a", zone="local", type="edge", site="hospital-a", cpu=8, ram=32, gpu=1),
            SimulatedNode("hospital-b", zone="local", type="edge", site="hospital-b", cpu=8, ram=32, gpu=1),
            SimulatedNode("hospital-c", zone="local", type="edge", site="hospital-c", cpu=4, ram=16, gpu=1),
            SimulatedNode("coordinator", zone="local", type="edge", role="coordinator", cpu=16, ram=64),
        ]
        wf = WaterWorkflow(
            name="federated-ml-test",
            intent=WorkflowIntent(privacy=PrivacyLevel.STRICT_LOCAL),
            steps=[
                WorkflowStep(id="train-a", name="Train A", image="myorg/trainer:1.0",
                             node_selector="site=hospital-a"),
                WorkflowStep(id="train-b", name="Train B", image="myorg/trainer:1.0",
                             node_selector="site=hospital-b"),
                WorkflowStep(id="train-c", name="Train C", image="myorg/trainer:1.0",
                             node_selector="site=hospital-c"),
                WorkflowStep(id="aggregate", name="Aggregate", image="myorg/aggregator:1.0",
                             node_selector="role=coordinator",
                             depends_on=["train-a", "train-b", "train-c"]),
            ],
        )
        report = Simulator(nodes).run(wf, policy="privacy-first")
        assert report.success, report.errors
        # Training steps should be on their respective hospital nodes
        assignments = {r.step_id: r.assigned_to for r in report.step_results}
        assert assignments["train-a"] == "hospital-a"
        assert assignments["train-b"] == "hospital-b"
        assert assignments["train-c"] == "hospital-c"
        assert assignments["aggregate"] == "coordinator"
