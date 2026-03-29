"""
WATER Scheduler — assigns workflow steps to nodes.

The Scheduler uses a simple but effective scoring algorithm:
  1. Filter nodes by node_selector (label match or explicit ID)
  2. Prefer nodes with GPU for image-processing steps
  3. Score remaining nodes by available CPU headroom and memory
  4. Return the best-fit node

This is the core intelligence of WATER's "allocation towards edge resources."
"""
from __future__ import annotations

from typing import Optional

from water.registry.node_registry import Node, NodeRegistry
from water.schema.workflow import WorkflowStep


class SchedulerError(Exception):
    pass


class Scheduler:
    """
    Assigns a WorkflowStep to the most suitable available Node.

    Design decisions documented here so mentors can see reasoning:
    - Bin-packing style: prefer nodes that have enough headroom but
      aren't over-provisioned (saves large nodes for heavy workloads).
    - GPU-awareness: steps whose image name contains 'gpu' or whose
      env declares USE_GPU=true get GPU nodes preferentially.
    - node_selector override: step can pin to a specific node ID or
      label (e.g., node_selector: "type=edge") for data-locality.
    """

    def __init__(self, registry: NodeRegistry) -> None:
        self.registry = registry

    def assign(self, step: WorkflowStep) -> Node:
        """
        Return the optimal node for `step`.
        Raises SchedulerError if no suitable node is found.
        """
        candidates = self.registry.list_available()

        if not candidates:
            raise SchedulerError("No available nodes in the registry")

        # 1. Apply node_selector filter
        if step.node_selector:
            candidates = self._apply_selector(candidates, step.node_selector)
            if not candidates:
                raise SchedulerError(
                    f"node_selector '{step.node_selector}' matched no available nodes"
                )

        # 2. GPU preference
        needs_gpu = (
            "gpu" in (step.image or "").lower()
            or step.env.get("USE_GPU", "").lower() in ("true", "1", "yes")
        )
        if needs_gpu:
            gpu_nodes = [n for n in candidates if n.gpu_count > 0]
            if gpu_nodes:
                candidates = gpu_nodes

        # 3. Score and pick best fit
        scored = sorted(candidates, key=lambda n: self._score(n), reverse=True)
        return scored[0]

    def _apply_selector(
        self, nodes: list[Node], selector: str
    ) -> list[Node]:
        """
        node_selector can be:
          - an exact node ID: "edge-01"
          - a label expression: "type=gpu" or "region=alaska"
        """
        # Exact ID match first
        by_id = [n for n in nodes if n.id == selector]
        if by_id:
            return by_id

        # Label match: "key=value"
        if "=" in selector:
            key, value = selector.split("=", 1)
            return [n for n in nodes if n.has_label(key.strip(), value.strip())]

        return []

    def _score(self, node: Node) -> float:
        """
        Higher score = better fit for the step.
        Simple heuristic: normalize CPU + memory, bonus for GPU.
        """
        cpu_score = min(node.cpu_cores / 32.0, 1.0)
        mem_score = min(node.memory_gb / 64.0, 1.0)
        gpu_bonus = 0.2 if node.gpu_count > 0 else 0.0
        return (cpu_score * 0.5) + (mem_score * 0.3) + gpu_bonus
