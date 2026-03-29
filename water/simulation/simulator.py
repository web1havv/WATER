"""
WATER Simulation Harness — test scheduling decisions without real infrastructure.

Purpose
-------
The simulation harness lets you evaluate and compare scheduling policies
on synthetic node configurations and workflow intents *without* requiring
real Docker hosts, SSH access, or live hardware.

This is directly useful for:
  - Validating that privacy constraints are correctly enforced before
    deploying to a real hospital network.
  - Benchmarking policy trade-offs (latency vs. privacy vs. throughput).
  - Demonstrating WATER's allocation logic in a reproducible way during
    proposal review — no infrastructure required to run the demo.

Architecture
------------
  SimulatedNode     — A fake compute node with configurable CPU/RAM/GPU/labels.
  SimulationClock   — Deterministic fake clock (avoids flakiness in tests).
  Simulator         — Runs a full workflow through the scheduler on sim nodes,
                      produces a SimulationReport with per-step assignments.

Usage::

    from water.simulation.simulator import Simulator, SimulatedNode
    from water.schema.workflow import WaterWorkflow, WorkflowIntent, PrivacyLevel
    import yaml

    nodes = [
        SimulatedNode("edge-a",  zone="local", cpu=8,  ram=16, gpu=1),
        SimulatedNode("edge-b",  zone="local", cpu=4,  ram=8,  gpu=0),
        SimulatedNode("cloud-1", zone="cloud", cpu=32, ram=128, gpu=0),
    ]

    with open("examples/federated_ml.yaml") as f:
        wf = WaterWorkflow(**yaml.safe_load(f))

    report = Simulator(nodes).run(wf, policy="privacy-first")
    report.print()
"""
from __future__ import annotations

import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

from water.registry.node_registry import Node, NodeRegistry, NodeStatus
from water.scheduler.scheduler import Scheduler, SchedulingPolicy, get_policy
from water.schema.workflow import WaterWorkflow, WorkflowIntent, WorkflowStep


# ---------------------------------------------------------------------------
# SimulatedNode
# ---------------------------------------------------------------------------

@dataclass
class SimulatedNode:
    """
    A synthetic compute node for simulation runs.

    Labels are built from the keyword arguments:
      zone, type, gpu, role, site — all become label key=value pairs.
    """
    node_id: str
    zone:    str = "local"
    type:    str = "edge"
    cpu:     int = 4
    ram:     float = 8.0
    gpu:     int = 0
    role:    str = ""
    site:    str = ""
    extra_labels: Dict[str, str] = field(default_factory=dict)

    def to_registry_node(self) -> Node:
        label_parts = [f"zone={self.zone}", f"type={self.type}"]
        if self.gpu > 0:
            label_parts.append("gpu=true")
        if self.role:
            label_parts.append(f"role={self.role}")
        if self.site:
            label_parts.append(f"site={self.site}")
        for k, v in self.extra_labels.items():
            label_parts.append(f"{k}={v}")

        return Node(
            id=self.node_id,
            hostname=f"sim-{self.node_id}",
            node_type=self.type,
            labels=",".join(label_parts),
            cpu_cores=self.cpu,
            memory_gb=self.ram,
            gpu_count=self.gpu,
            status=NodeStatus.ONLINE,
        )


# ---------------------------------------------------------------------------
# SimulationReport
# ---------------------------------------------------------------------------

@dataclass
class StepResult:
    step_id:      str
    wave:         int
    assigned_to:  str
    node_type:    str
    node_zone:    str
    node_labels:  str
    policy_used:  str


@dataclass
class SimulationReport:
    workflow_name: str
    intent:        WorkflowIntent
    policy_name:   str
    step_results:  List[StepResult] = field(default_factory=list)
    errors:        List[str]        = field(default_factory=list)
    elapsed_ms:    float            = 0.0

    @property
    def success(self) -> bool:
        return len(self.errors) == 0

    def print(self) -> None:
        print(f"\n{'='*60}")
        print(f"  WATER Simulation Report")
        print(f"  Workflow  : {self.workflow_name}")
        print(f"  Policy    : {self.policy_name}")
        print(f"  Privacy   : {self.intent.privacy.value}")
        print(f"  Latency   : {self.intent.latency.value}")
        print(f"  Compute   : {self.intent.compute.value}")
        print(f"  Status    : {'PASS' if self.success else 'FAIL'}")
        print(f"  Elapsed   : {self.elapsed_ms:.1f}ms")
        print(f"{'='*60}")

        if self.step_results:
            print(f"\n  {'Step':<30} {'Wave':>4}  {'Node':<18} {'Zone':<10} Labels")
            print(f"  {'-'*80}")
            for r in self.step_results:
                print(
                    f"  {r.step_id:<30} {r.wave:>4}  {r.assigned_to:<18} "
                    f"{r.node_zone:<10} {r.node_labels}"
                )

        if self.errors:
            print(f"\n  ERRORS:")
            for e in self.errors:
                print(f"    ✗ {e}")

        print()

    def assert_privacy_respected(self) -> None:
        """
        Assert that every step with a strict-local intent was assigned to
        a local-zone node.  Raises AssertionError if violated.
        """
        if self.intent.privacy.value != "strict-local":
            return
        for r in self.step_results:
            assert r.node_zone == "local", (
                f"Privacy violation: step '{r.step_id}' with strict-local intent "
                f"was assigned to zone='{r.node_zone}' node '{r.assigned_to}'"
            )


# ---------------------------------------------------------------------------
# Simulator
# ---------------------------------------------------------------------------

class Simulator:
    """
    Runs a WATER workflow through the scheduler on a synthetic node fleet.

    No Docker, no SSH, no live hardware required.  The simulator is
    deterministic and suitable for use in CI / automated tests.

    Example::

        nodes = [
            SimulatedNode("edge-01", zone="local", cpu=8, ram=32, gpu=2),
            SimulatedNode("cloud-01", zone="cloud", cpu=32, ram=128),
        ]
        report = Simulator(nodes).run(workflow, policy="privacy-first")
        report.print()
        report.assert_privacy_respected()
    """

    def __init__(self, nodes: List[SimulatedNode]) -> None:
        self.sim_nodes = nodes

    def run(
        self,
        workflow: WaterWorkflow,
        policy: Optional[str] = None,
    ) -> SimulationReport:
        """
        Simulate scheduling *workflow* steps across the synthetic node fleet.

        Args:
            workflow: A WaterWorkflow loaded from a YAML intent file.
            policy:   Policy name ('privacy-first', 'latency-optimized', 'default').
                      If omitted, uses 'default'.
        Returns:
            SimulationReport with per-step assignments and any errors.
        """
        start   = time.monotonic()
        intent  = workflow.intent
        pol_obj = get_policy(policy or "default")

        report = SimulationReport(
            workflow_name=workflow.name,
            intent=intent,
            policy_name=pol_obj.name(),
        )

        with tempfile.TemporaryDirectory() as tmp:
            registry = self._build_registry(tmp)
            scheduler = Scheduler(registry, policy=pol_obj)
            waves     = workflow.execution_order()

            for wave_idx, wave in enumerate(waves):
                for step_id in wave:
                    step = workflow.get_step(step_id)
                    if step is None:
                        report.errors.append(f"Step '{step_id}' not found in workflow")
                        continue
                    try:
                        node = scheduler.assign(step, intent)
                        report.step_results.append(StepResult(
                            step_id=step_id,
                            wave=wave_idx,
                            assigned_to=node.id,
                            node_type=node.node_type,
                            node_zone=self._label_value(node, "zone"),
                            node_labels=node.labels,
                            policy_used=pol_obj.name(),
                        ))
                    except Exception as exc:
                        report.errors.append(f"Step '{step_id}': {exc}")

        report.elapsed_ms = (time.monotonic() - start) * 1000
        return report

    def compare_policies(
        self,
        workflow: WaterWorkflow,
        policies: Optional[List[str]] = None,
    ) -> Dict[str, SimulationReport]:
        """
        Run the same workflow under multiple policies and return all reports.

        Useful for demonstrating the effect of different scheduling strategies
        side-by-side.
        """
        if policies is None:
            policies = ["privacy-first", "latency-optimized", "default"]
        return {p: self.run(workflow, policy=p) for p in policies}

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _build_registry(self, tmp_dir: str) -> NodeRegistry:
        registry = NodeRegistry(db_path=Path(tmp_dir) / "sim.sqlite")
        for sim_node in self.sim_nodes:
            registry.register(sim_node.to_registry_node())
        return registry

    @staticmethod
    def _label_value(node: Node, key: str) -> str:
        for part in (node.labels or "").split(","):
            k, _, v = part.partition("=")
            if k.strip() == key:
                return v.strip()
        return ""
