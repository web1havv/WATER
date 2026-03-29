"""
WATER CLI — command-line interface for the WATER framework.

Commands:
  water nodes ls               List all registered nodes
  water nodes add              Register a new node
  water nodes rm <id>          Remove a node
  water nodes ping <id>        Send heartbeat to a node

  water submit <workflow.yaml> Submit a workflow for execution
  water status <run_id>        Show run status
  water runs                   List all runs

  water convert <study.graphml> [output.yaml]
                               Convert concore GraphML to WATER YAML

Usage:
  pip install water
  water nodes ls
  water submit examples/niffler_dicom_pipeline.yaml
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import yaml

# ── Inline lightweight client (no requests dep required) ───────────
# The CLI talks to the WATER API or directly uses the Python modules.
# For the proposal demo, it uses the Python modules directly.


def cmd_nodes_ls(args):
    from water.registry.node_registry import NodeRegistry
    registry = NodeRegistry()
    nodes = registry.list_all()
    if not nodes:
        print("No nodes registered. Use: water nodes add --id <id> --host <hostname>")
        return
    print(f"{'ID':<20} {'TYPE':<8} {'HOST':<30} {'CPU':>4} {'MEM':>7} {'GPU':>4} {'STATUS':<12} LABELS")
    print("-" * 100)
    for n in nodes:
        print(
            f"{n.id:<20} {n.node_type:<8} {n.hostname:<30} "
            f"{n.cpu_cores:>4} {n.memory_gb:>6.1f}G {n.gpu_count:>4} "
            f"{n.status:<12} {n.labels}"
        )


def cmd_nodes_add(args):
    from water.registry.node_registry import Node, NodeRegistry
    node = Node(
        id=args.id,
        hostname=args.host,
        ssh_user=args.user,
        ssh_port=args.port,
        node_type=args.type,
        labels=args.labels or "",
        cpu_cores=args.cpu,
        memory_gb=args.mem,
        gpu_count=args.gpu,
        data_root=args.data_root,
    )
    NodeRegistry().register(node)
    print(f"Registered node '{args.id}' ({args.type}) at {args.host}")


def cmd_nodes_rm(args):
    from water.registry.node_registry import NodeRegistry
    reg = NodeRegistry()
    if not reg.get(args.id):
        print(f"Node '{args.id}' not found.")
        sys.exit(1)
    reg.remove(args.id)
    print(f"Removed node '{args.id}'")


def cmd_nodes_ping(args):
    from water.registry.node_registry import NodeRegistry
    reg = NodeRegistry()
    if not reg.get(args.id):
        print(f"Node '{args.id}' not found.")
        sys.exit(1)
    reg.heartbeat(args.id)
    print(f"Heartbeat sent to '{args.id}'")


def cmd_submit(args):
    from water.engine.execution_engine import ExecutionEngine
    from water.registry.node_registry import NodeRegistry
    from water.schema.workflow import WaterWorkflow

    p = Path(args.workflow)
    if not p.exists():
        print(f"File not found: {args.workflow}")
        sys.exit(1)

    raw = yaml.safe_load(p.read_text())
    try:
        workflow = WaterWorkflow(**raw)
    except Exception as exc:
        print(f"Workflow parse error: {exc}")
        sys.exit(1)

    print(f"Submitting workflow: {workflow.name} ({len(workflow.steps)} steps)")
    waves = workflow.execution_order()
    print(f"Execution plan: {len(waves)} wave(s)")
    for i, w in enumerate(waves, 1):
        print(f"  Wave {i}: {w}")

    registry = NodeRegistry()
    if not registry.list_available():
        print("WARNING: No available nodes. Register nodes first with: water nodes add")
        print("Showing dry-run (scheduler simulation) only.")
        from water.scheduler.scheduler import Scheduler
        scheduler = Scheduler(registry)
        print("\nDry-run assignments:")
        for step in workflow.steps:
            try:
                node = scheduler.assign(step)
                print(f"  {step.id} -> {node.id}")
            except Exception as e:
                print(f"  {step.id} -> UNSCHEDULABLE ({e})")
        return

    engine = ExecutionEngine(registry)
    run = engine.run(workflow)
    print("\n" + run.summary())


def cmd_status(args):
    # In a real deployment this would query the API.
    # For the demo, show what's in the run store.
    print("Run store is in-memory per API session.")
    print("Use 'water submit' to create a run, then check via GET /workflows/<run_id>")
    print("SQLite persistence of runs is on the Week 6 roadmap.")


def cmd_runs(args):
    print("Run listing requires the WATER API to be running.")
    print("Start with: uvicorn water.api.app:app --reload")
    print("Then: curl http://localhost:8000/workflows")


def cmd_convert(args):
    from water.converters.graphml_to_water import GraphMLConverter
    converter = GraphMLConverter()
    try:
        yaml_str = converter.convert(args.graphml)
    except Exception as exc:
        print(f"Conversion failed: {exc}")
        sys.exit(1)

    if args.output:
        Path(args.output).write_text(yaml_str)
        print(f"Written to {args.output}")
    else:
        print(yaml_str)


# ── Argument parser ───────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="water",
        description="WATER: Workflow Allocation Towards Edge Resources",
    )
    sub = parser.add_subparsers(dest="command", metavar="<command>")

    # nodes
    nodes_p = sub.add_parser("nodes", help="Manage edge/cloud nodes")
    nodes_sub = nodes_p.add_subparsers(dest="nodes_cmd", metavar="<subcommand>")

    nodes_sub.add_parser("ls", help="List registered nodes")

    add_p = nodes_sub.add_parser("add", help="Register a new node")
    add_p.add_argument("--id",        required=True)
    add_p.add_argument("--host",      required=True)
    add_p.add_argument("--user",      default="water")
    add_p.add_argument("--port",      type=int, default=22)
    add_p.add_argument("--type",      choices=["edge", "cloud", "local"], default="edge")
    add_p.add_argument("--labels",    default="")
    add_p.add_argument("--cpu",       type=int, default=4)
    add_p.add_argument("--mem",       type=float, default=8.0)
    add_p.add_argument("--gpu",       type=int, default=0)
    add_p.add_argument("--data-root", default="/tmp/water", dest="data_root")

    rm_p = nodes_sub.add_parser("rm", help="Remove a node")
    rm_p.add_argument("id")

    ping_p = nodes_sub.add_parser("ping", help="Send heartbeat")
    ping_p.add_argument("id")

    # submit
    submit_p = sub.add_parser("submit", help="Submit a workflow YAML")
    submit_p.add_argument("workflow", help="Path to workflow.yaml")

    # status
    status_p = sub.add_parser("status", help="Show run status")
    status_p.add_argument("run_id", nargs="?", default=None)

    # runs
    sub.add_parser("runs", help="List all workflow runs")

    # convert
    conv_p = sub.add_parser("convert", help="Convert concore GraphML to WATER YAML")
    conv_p.add_argument("graphml")
    conv_p.add_argument("output", nargs="?", default=None)

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "nodes":
        if args.nodes_cmd == "ls":
            cmd_nodes_ls(args)
        elif args.nodes_cmd == "add":
            cmd_nodes_add(args)
        elif args.nodes_cmd == "rm":
            cmd_nodes_rm(args)
        elif args.nodes_cmd == "ping":
            cmd_nodes_ping(args)
        else:
            parser.parse_args(["nodes", "--help"])

    elif args.command == "submit":
        cmd_submit(args)
    elif args.command == "status":
        cmd_status(args)
    elif args.command == "runs":
        cmd_runs(args)
    elif args.command == "convert":
        cmd_convert(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
