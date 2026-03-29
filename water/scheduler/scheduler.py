"""
WATER Scheduler — pluggable policy engine for workflow-to-node allocation.

Architecture
------------
The scheduler is built around a *Policy* abstraction.  Each policy provides
two methods:

  filter(nodes, step, intent) -> List[Node]
      Hard constraints — nodes that fail are removed from the candidate set
      entirely, not just scored lower.  Privacy enforcement lives here.

  score(node, step, intent) -> float
      Soft scoring — higher is better.  Used to rank the filtered candidates.

Built-in policies
-----------------
  PrivacyFirstPolicy   — Strict privacy enforcement (HIPAA edge-anchoring).
                         strict-local  → cloud nodes removed entirely.
                         edge-only     → cloud deprioritised but not removed.
  LatencyOptimizedPolicy — Prefers edge nodes; penalises cloud round-trip latency.
  DefaultPolicy          — Balanced CPU + memory + GPU scoring.

Adding a new policy requires only implementing the SchedulingPolicy interface —
no changes to the Scheduler core.  This is the "pluggable policies" architecture
that keeps WATER extensible as new healthcare compliance requirements emerge.
"""
from __future__ import annotations

import abc
from typing import List, Optional

from water.registry.node_registry import Node, NodeRegistry
from water.schema.workflow import ComputeProfile, PrivacyLevel, WorkflowIntent, WorkflowStep


# ---------------------------------------------------------------------------
# Policy interface
# ---------------------------------------------------------------------------

class SchedulingPolicy(abc.ABC):
    """Base class for all WATER scheduling policies."""

    @abc.abstractmethod
    def filter(
        self,
        nodes: List[Node],
        step: WorkflowStep,
        intent: WorkflowIntent,
    ) -> List[Node]:
        """
        Return the subset of *nodes* that are eligible for *step*.

        Nodes removed here are disqualified — they will never be scored.
        This is where hard constraints (privacy, compliance) belong.
        """

    @abc.abstractmethod
    def score(
        self,
        node: Node,
        step: WorkflowStep,
        intent: WorkflowIntent,
    ) -> float:
        """
        Return a non-negative score for *node* given *step* and *intent*.
        Higher score = better fit.  Called only on nodes that passed filter().
        """

    def name(self) -> str:
        return self.__class__.__name__


# ---------------------------------------------------------------------------
# Built-in policies
# ---------------------------------------------------------------------------

class PrivacyFirstPolicy(SchedulingPolicy):
    """
    Enforces HIPAA-aligned data-locality constraints as *hard disqualifiers*,
    not scoring penalties.  This matches the mentor's explicit requirement:

        "privacy: strict-local removes cloud nodes from the candidate set
         entirely before any scoring happens."

    Privacy levels:
      strict-local  → only nodes tagged zone=local survive the filter.
      edge-only     → cloud nodes survive but receive a score penalty.
      public        → no zone filter applied.
    """

    def filter(
        self,
        nodes: List[Node],
        step: WorkflowStep,
        intent: WorkflowIntent,
    ) -> List[Node]:
        if intent.privacy == PrivacyLevel.STRICT_LOCAL:
            local_nodes = [n for n in nodes if n.has_label("zone", "local")]
            return local_nodes if local_nodes else []

        if intent.privacy == PrivacyLevel.EDGE_ONLY:
            edge_nodes = [
                n for n in nodes
                if n.has_label("type", "edge") or n.has_label("zone", "local")
            ]
            return edge_nodes if edge_nodes else nodes  # fallback to all

        return nodes  # public — no zone constraint

    def score(
        self,
        node: Node,
        step: WorkflowStep,
        intent: WorkflowIntent,
    ) -> float:
        base = _resource_score(node)
        # Edge-only: small boost for local nodes as tie-breaker
        if intent.privacy == PrivacyLevel.EDGE_ONLY:
            if node.has_label("zone", "local") or node.has_label("type", "edge"):
                base += 0.15
        return base


class LatencyOptimizedPolicy(SchedulingPolicy):
    """
    Minimises task start-up latency by preferring edge nodes geographically
    close to the data source.

    Rationale: for interactive workflows (real-time imaging feedback,
    neuromodulation control loops), network round-trip to a cloud node can
    dominate total execution time.  Edge-local execution avoids this.
    """

    def filter(
        self,
        nodes: List[Node],
        step: WorkflowStep,
        intent: WorkflowIntent,
    ) -> List[Node]:
        # Latency policy always applies privacy constraints as a prerequisite
        return PrivacyFirstPolicy().filter(nodes, step, intent)

    def score(
        self,
        node: Node,
        step: WorkflowStep,
        intent: WorkflowIntent,
    ) -> float:
        base = _resource_score(node)
        # Latency dominates: strong boost for edge/local, strong penalty for cloud
        if node.has_label("type", "edge") or node.has_label("zone", "local"):
            base += 0.80
        elif node.has_label("type", "cloud"):
            base -= 0.60
        return max(base, 0.0)


class DefaultPolicy(SchedulingPolicy):
    """
    Balanced scoring: GPU-awareness + resource headroom + privacy soft-preference.
    Used when no explicit policy is configured.
    """

    def filter(
        self,
        nodes: List[Node],
        step: WorkflowStep,
        intent: WorkflowIntent,
    ) -> List[Node]:
        # Delegate privacy hard-constraints to PrivacyFirstPolicy
        candidates = PrivacyFirstPolicy().filter(nodes, step, intent)

        # Compute profile filter
        if intent.compute == ComputeProfile.GPU_REQUIRED:
            gpu_nodes = [n for n in candidates if n.gpu_count > 0]
            if gpu_nodes:
                return gpu_nodes

        if intent.compute == ComputeProfile.CPU_HEAVY:
            heavy = [n for n in candidates if n.cpu_cores >= 8 and n.memory_gb >= 32]
            if heavy:
                return heavy

        return candidates

    def score(
        self,
        node: Node,
        step: WorkflowStep,
        intent: WorkflowIntent,
    ) -> float:
        base = _resource_score(node)

        # GPU bonus when step explicitly requests it
        needs_gpu = (
            "gpu" in (step.image or "").lower()
            or step.env.get("USE_GPU", "").lower() in ("true", "1", "yes")
            or intent.compute == ComputeProfile.GPU_REQUIRED
        )
        if needs_gpu and node.gpu_count > 0:
            base += 0.25

        return base


# ---------------------------------------------------------------------------
# Policy registry  (add new policies here — no other file needs changing)
# ---------------------------------------------------------------------------

_POLICY_REGISTRY: dict[str, type[SchedulingPolicy]] = {
    "privacy-first":      PrivacyFirstPolicy,
    "latency-optimized":  LatencyOptimizedPolicy,
    "default":            DefaultPolicy,
}


def get_policy(name: str) -> SchedulingPolicy:
    """Return a policy instance by name.  Raises KeyError for unknown names."""
    cls = _POLICY_REGISTRY.get(name)
    if cls is None:
        available = list(_POLICY_REGISTRY.keys())
        raise KeyError(f"Unknown policy '{name}'. Available: {available}")
    return cls()


def available_policies() -> List[str]:
    return list(_POLICY_REGISTRY.keys())


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------

class SchedulerError(Exception):
    pass


class Scheduler:
    """
    Assigns a WorkflowStep to the most suitable available Node.

    The scheduler is policy-driven: swap the policy to change allocation
    behaviour without touching the core engine.

    Usage::

        registry = NodeRegistry()
        scheduler = Scheduler(registry, policy=PrivacyFirstPolicy())
        node = scheduler.assign(step, intent)

    Or select by name::

        scheduler = Scheduler(registry, policy=get_policy("latency-optimized"))
    """

    def __init__(
        self,
        registry: NodeRegistry,
        policy: Optional[SchedulingPolicy] = None,
    ) -> None:
        self.registry = registry
        self.policy: SchedulingPolicy = policy or DefaultPolicy()

    def assign(self, step: WorkflowStep, intent: Optional[WorkflowIntent] = None) -> Node:
        """
        Return the optimal node for *step* given *intent*.
        Raises SchedulerError if no suitable node is found.
        """
        if intent is None:
            intent = WorkflowIntent()

        all_available = self.registry.list_available()
        if not all_available:
            raise SchedulerError("No available nodes in the registry")

        # 1. Apply node_selector override (explicit pin takes precedence over policy)
        candidates = all_available
        if step.node_selector:
            candidates = self._apply_selector(all_available, step.node_selector)
            if not candidates:
                raise SchedulerError(
                    f"node_selector '{step.node_selector}' matched no available nodes"
                )
        else:
            # 2. Apply policy hard filters
            candidates = self.policy.filter(candidates, step, intent)
            if not candidates:
                raise SchedulerError(
                    f"Policy '{self.policy.name()}' filtered out all available nodes "
                    f"(privacy={intent.privacy}, compute={intent.compute})"
                )

        # 3. Score remaining candidates and pick the best
        scored = sorted(
            candidates,
            key=lambda n: self.policy.score(n, step, intent),
            reverse=True,
        )
        return scored[0]

    def _apply_selector(self, nodes: List[Node], selector: str) -> List[Node]:
        """
        node_selector supports:
          - Exact node ID:     "edge-01"
          - Label expression:  "zone=local"  or  "gpu=true"
        """
        by_id = [n for n in nodes if n.id == selector]
        if by_id:
            return by_id

        if "=" in selector:
            key, value = selector.split("=", 1)
            return [n for n in nodes if n.has_label(key.strip(), value.strip())]

        return []


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _resource_score(node: Node) -> float:
    """Normalised resource headroom score — higher is a better-provisioned node."""
    cpu_score = min(node.cpu_cores / 32.0, 1.0)
    mem_score = min(node.memory_gb / 64.0, 1.0)
    return (cpu_score * 0.55) + (mem_score * 0.45)
