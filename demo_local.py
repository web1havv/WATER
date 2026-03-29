"""
WATER Local Demo — runs without Docker or SSH to prove core logic works.

This demo:
  1. Parses the YAML workflow definition (schema validation)
  2. Registers three virtual nodes into the SQLite NodeRegistry
  3. Runs the Scheduler to assign each step to a node
  4. Shows the execution order (topological sort)
  5. Shows the full StepResult summary

Run with: python3 demo_local.py
"""
import sys
import yaml
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

sys.path.insert(0, str(Path(__file__).parent))

from water.schema.workflow import WaterWorkflow
from water.registry.node_registry import Node, NodeRegistry, NodeStatus
from water.scheduler.scheduler import Scheduler


def main():
    print("=" * 65)
    print("  WATER: Workflow Allocation Towards Edge Resources — Demo")
    print("=" * 65)

    # ── 1. Parse workflow YAML ──────────────────────────────────────
    yaml_path = Path(__file__).parent / "examples" / "niffler_dicom_pipeline.yaml"
    with open(yaml_path) as f:
        raw = yaml.safe_load(f)

    workflow = WaterWorkflow(**raw)
    print(f"\n[1] Loaded workflow: '{workflow.name}' ({len(workflow.steps)} steps)")
    for step in workflow.steps:
        print(f"    • {step.id:25s}  image={step.image}  deps={step.depends_on}")

    # ── 2. Register virtual nodes ───────────────────────────────────
    registry = NodeRegistry(db_path=Path("/tmp/water_demo_registry.db"))

    nodes = [
        Node(
            id="edge-pacs-01",
            hostname="192.168.1.10",
            node_type="edge",
            labels="role=pacs-gateway,region=alaska-anchorage",
            cpu_cores=4,
            memory_gb=8.0,
            gpu_count=0,
        ),
        Node(
            id="edge-gpu-01",
            hostname="192.168.1.20",
            node_type="edge",
            labels="role=gpu-worker,region=alaska-anchorage",
            cpu_cores=16,
            memory_gb=32.0,
            gpu_count=2,
        ),
        Node(
            id="cloud-aws-01",
            hostname="ec2-54-23-44-55.compute-1.amazonaws.com",
            node_type="cloud",
            labels="role=cloud-indexer,provider=aws",
            cpu_cores=8,
            memory_gb=16.0,
            gpu_count=0,
        ),
    ]

    for node in nodes:
        registry.register(node)

    print(f"\n[2] Registered {len(nodes)} nodes:")
    for n in registry.list_all():
        print(
            f"    • {n.id:20s}  type={n.node_type:6s}  "
            f"cpu={n.cpu_cores:2d}  mem={n.memory_gb:5.1f}GB  "
            f"gpu={n.gpu_count}  labels={n.labels}"
        )

    # ── 3. Topological sort ─────────────────────────────────────────
    waves = workflow.execution_order()
    print(f"\n[3] Execution plan ({len(waves)} waves):")
    for i, wave in enumerate(waves, 1):
        print(f"    Wave {i}: {wave}")

    # ── 4. Scheduler assignment ─────────────────────────────────────
    scheduler = Scheduler(registry)
    print("\n[4] Scheduler assignments:")
    for step in workflow.steps:
        try:
            node = scheduler.assign(step)
            score = scheduler._score(node)
            print(
                f"    {step.id:25s} -> {node.id:20s}  "
                f"(score={score:.3f}, selector='{step.node_selector or 'any'}')"
            )
        except Exception as exc:
            print(f"    {step.id:25s} -> ERROR: {exc}")

    # ── 5. Data transfer plan ───────────────────────────────────────
    print(f"\n[5] Data transfer plan ({len(workflow.transfers)} transfers):")
    for t in workflow.transfers:
        print(
            f"    {t.from_step}.{t.from_port:15s} "
            f"--[{t.protocol.value:5s}]--> "
            f"{t.to_step}.{t.to_port}"
        )

    print("\n" + "=" * 65)
    print("  Demo complete. Core WATER logic validated without Docker/SSH.")
    print("  Next: connect to real nodes and run live healthcare workflows.")
    print("=" * 65)


if __name__ == "__main__":
    main()
