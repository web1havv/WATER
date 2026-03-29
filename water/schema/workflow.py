"""
WATER Workflow Schema — Pydantic v2 models for declarative workflow intents.

A WATER workflow is a YAML "Intent" file. It tells WATER *what* to run and
*what constraints matter* (privacy, latency, compute). WATER decides *where*
and *how* — completely independent of the pipeline inside the containers.

Niffler, CONTROL-CORE, Nextflow, custom ML jobs — all expressed identically.
WATER only handles the allocation and execution fabric, not the medical logic.
"""
from __future__ import annotations

from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel, Field, field_validator


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class TransferProtocol(str, Enum):
    LOCAL = "local"
    SSH   = "ssh"
    SFTP  = "sftp"
    NFS   = "nfs"
    S3    = "s3"


class NodeType(str, Enum):
    EDGE  = "edge"
    CLOUD = "cloud"
    LOCAL = "local"


class StepStatus(str, Enum):
    PENDING   = "pending"
    RUNNING   = "running"
    COMPLETED = "completed"
    FAILED    = "failed"
    SKIPPED   = "skipped"


class PrivacyLevel(str, Enum):
    """
    Hard constraint evaluated *before* any scoring.

    strict-local  — Only nodes tagged zone=local are eligible.
                    Cloud nodes are disqualified entirely, not penalised.
                    Use for workflows that must not leave the hospital network.
    edge-only     — Edge nodes strongly preferred; cloud only if no edge available.
    public        — No geographic or zone restriction.

    This mirrors the mentor's explicit requirement: privacy must be a
    "hard disqualifier, not a scoring penalty."
    """
    STRICT_LOCAL = "strict-local"
    EDGE_ONLY    = "edge-only"
    PUBLIC       = "public"


class LatencyProfile(str, Enum):
    """
    Drives node-proximity preference in the allocation engine.

    interactive  — Prefer the node with lowest round-trip latency.
    batch        — Optimise for throughput; minutes of startup time acceptable.
    best-effort  — No SLA; scheduler free to pick any fit node.
    """
    INTERACTIVE = "interactive"
    BATCH       = "batch"
    BEST_EFFORT = "best-effort"


class ComputeProfile(str, Enum):
    """
    Coarse compute requirement used when no explicit node_selector is set.
    The scheduler maps this to minimum CPU/RAM thresholds from node telemetry.
    """
    CPU_LIGHT    = "cpu-light"     # < 4 cores, < 8 GB RAM
    CPU_HEAVY    = "cpu-heavy"     # ≥ 8 cores, ≥ 32 GB RAM
    GPU_REQUIRED = "gpu-required"  # ≥ 1 GPU required


# ---------------------------------------------------------------------------
# Workflow Intent  (the declarative contract submitted by the researcher)
# ---------------------------------------------------------------------------

class WorkflowIntent(BaseModel):
    """
    Declarative intent submitted by a researcher alongside the workflow steps.

    The allocation engine evaluates the intent against live node telemetry —
    no pipeline-specific logic is required. The same intent schema works for
    any containerised healthcare workload.

    Example YAML fragment::

        intent:
          privacy: strict-local
          latency: batch
          compute: gpu-required
          tags:
            project: radiology-ai
            region: alaska
            domain: imaging
    """
    privacy: PrivacyLevel = Field(
        default=PrivacyLevel.PUBLIC,
        description=(
            "Hard privacy constraint. strict-local removes all non-local nodes "
            "from the candidate set before scoring begins."
        ),
    )
    latency: LatencyProfile = Field(
        default=LatencyProfile.BATCH,
        description="Latency SLA that drives node-proximity scoring.",
    )
    compute: ComputeProfile = Field(
        default=ComputeProfile.CPU_LIGHT,
        description="Minimum compute profile required; used when node_selector is absent.",
    )
    tags: Dict[str, str] = Field(
        default_factory=dict,
        description="Arbitrary metadata attached to every run record for audit/search.",
    )


# ---------------------------------------------------------------------------
# Step-level models
# ---------------------------------------------------------------------------

class DataPort(BaseModel):
    """Named input or output data port for a workflow step."""
    name:        str           = Field(..., description="Logical channel name")
    path:        str           = Field(..., description="Path relative to the node's data root")
    description: Optional[str] = None


class WorkflowStep(BaseModel):
    """
    A single executable step — maps to one OCI container on one allocated node.

    WATER is entirely agnostic to the container's contents.  The container may
    run a DICOM extractor, a genomics aligner, a federated-learning trainer, or
    anything else.  WATER only manages WHERE it runs and HOW data reaches it.
    """
    id:    str = Field(..., description="Unique step identifier (no spaces)")
    name:  str = Field(..., description="Human-readable step name")
    image: str = Field(..., description="OCI / Docker image URI")
    command: Optional[List[str]] = Field(
        None, description="Command override; defaults to the container's CMD/ENTRYPOINT"
    )
    inputs:  List[DataPort] = Field(default_factory=list)
    outputs: List[DataPort] = Field(default_factory=list)
    node_selector: Optional[str] = Field(
        None,
        description=(
            "Pin this step to a specific node ID or label expression. "
            "Examples: 'edge-01', 'zone=local', 'gpu=true'. "
            "Overrides intent.compute for this step."
        ),
    )
    env: Dict[str, str] = Field(
        default_factory=dict,
        description="Environment variables injected at container start.",
    )
    retry_count:     int           = Field(default=2, ge=0, le=10)
    timeout_seconds: Optional[int] = Field(None, ge=1)
    depends_on: List[str] = Field(
        default_factory=list,
        description="IDs of steps that must complete before this step starts.",
    )

    @field_validator("id")
    @classmethod
    def id_no_spaces(cls, v: str) -> str:
        if " " in v:
            raise ValueError("Step id must not contain spaces")
        return v


class DataTransfer(BaseModel):
    """Describes how data moves between two steps that may be on different nodes."""
    from_step: str = Field(..., description="Source step ID")
    from_port: str = Field(..., description="Output port name on the source step")
    to_step:   str = Field(..., description="Destination step ID")
    to_port:   str = Field(..., description="Input port name on the destination step")
    protocol: TransferProtocol = Field(
        default=TransferProtocol.SSH,
        description="Transfer protocol; WATER may override if both steps share a node.",
    )


# ---------------------------------------------------------------------------
# Root workflow model
# ---------------------------------------------------------------------------

class WaterWorkflow(BaseModel):
    """
    The complete workflow intent submitted to WATER.

    Serialised from / to YAML.  Minimal example::

        name: my-pipeline
        intent:
          privacy: strict-local
          latency: batch
        steps:
          - id: preprocess
            image: myorg/preprocess:1.0
            outputs:
              - name: features
                path: out/features/
          - id: train
            image: myorg/train:1.0
            depends_on: [preprocess]
            inputs:
              - name: features
                path: out/features/
    """
    name:        str                 = Field(..., description="Workflow name / job label")
    version:     str                 = Field(default="1.0")
    description: Optional[str]       = None
    intent:      WorkflowIntent      = Field(
        default_factory=WorkflowIntent,
        description="Declarative resource and privacy intent for this entire workflow.",
    )
    steps:     List[WorkflowStep]   = Field(..., min_length=1)
    transfers: List[DataTransfer]   = Field(default_factory=list)
    global_env: Dict[str, str]      = Field(
        default_factory=dict,
        description="Environment variables injected into every step container.",
    )

    def execution_order(self) -> List[List[str]]:
        """
        Topological sort of steps respecting depends_on relationships.
        Returns a list of *waves*; every step inside a wave can run in parallel.
        """
        from collections import defaultdict, deque

        in_degree: Dict[str, int]       = {s.id: 0 for s in self.steps}
        graph:     Dict[str, List[str]] = defaultdict(list)

        for step in self.steps:
            for dep in step.depends_on:
                graph[dep].append(step.id)
                in_degree[step.id] += 1

        queue = deque(sid for sid, deg in in_degree.items() if deg == 0)
        waves: List[List[str]] = []

        while queue:
            wave = list(queue)
            waves.append(wave)
            queue.clear()
            for sid in wave:
                for neighbor in graph[sid]:
                    in_degree[neighbor] -= 1
                    if in_degree[neighbor] == 0:
                        queue.append(neighbor)

        return waves

    def get_step(self, step_id: str) -> Optional[WorkflowStep]:
        return next((s for s in self.steps if s.id == step_id), None)
