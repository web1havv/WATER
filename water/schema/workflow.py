"""
WATER Workflow Schema — Pydantic v2 models for workflow definitions.

A WATER workflow is defined as a YAML file describing:
  - steps: ordered list of tasks to execute
  - nodes: available edge/cloud compute nodes
  - data: input/output path mappings between steps
"""
from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator


class TransferProtocol(str, Enum):
    LOCAL = "local"
    SSH = "ssh"
    SFTP = "sftp"
    NFS = "nfs"
    S3 = "s3"


class NodeType(str, Enum):
    EDGE = "edge"
    CLOUD = "cloud"
    LOCAL = "local"


class StepStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class DataPort(BaseModel):
    """A named input or output data port for a workflow step."""
    name: str = Field(..., description="Logical name for this data channel")
    path: str = Field(..., description="Filesystem path relative to the node's data root")
    description: Optional[str] = None


class WorkflowStep(BaseModel):
    """A single executable step in the WATER workflow pipeline."""
    id: str = Field(..., description="Unique identifier for this step")
    name: str = Field(..., description="Human-readable step name")
    image: str = Field(..., description="Docker image to run for this step")
    command: Optional[List[str]] = Field(
        None, description="Command override; defaults to Docker CMD"
    )
    inputs: List[DataPort] = Field(default_factory=list)
    outputs: List[DataPort] = Field(default_factory=list)
    node_selector: Optional[str] = Field(
        None,
        description="Node ID or label to pin this step to a specific node"
    )
    env: Dict[str, str] = Field(
        default_factory=dict,
        description="Environment variables injected into the container"
    )
    retry_count: int = Field(default=2, ge=0, le=10)
    timeout_seconds: Optional[int] = Field(None, ge=1)
    depends_on: List[str] = Field(
        default_factory=list,
        description="Step IDs that must complete before this step starts"
    )

    @field_validator("id")
    @classmethod
    def id_no_spaces(cls, v: str) -> str:
        if " " in v:
            raise ValueError("Step id must not contain spaces")
        return v


class DataTransfer(BaseModel):
    """Defines how data moves between two steps across nodes."""
    from_step: str = Field(..., description="Source step ID")
    from_port: str = Field(..., description="Output port name on the source step")
    to_step: str = Field(..., description="Destination step ID")
    to_port: str = Field(..., description="Input port name on the destination step")
    protocol: TransferProtocol = Field(
        default=TransferProtocol.SSH,
        description="Transfer protocol to use when nodes differ"
    )


class WaterWorkflow(BaseModel):
    """Root workflow definition — serialized from/to YAML."""
    name: str = Field(..., description="Workflow name (also used as job label)")
    version: str = Field(default="1.0")
    description: Optional[str] = None
    steps: List[WorkflowStep] = Field(..., min_length=1)
    transfers: List[DataTransfer] = Field(default_factory=list)
    global_env: Dict[str, str] = Field(
        default_factory=dict,
        description="Environment variables shared across all steps"
    )

    def execution_order(self) -> List[List[str]]:
        """
        Topological sort of steps respecting depends_on.
        Returns a list of waves, where each wave can run in parallel.
        """
        from collections import defaultdict, deque

        in_degree: Dict[str, int] = {s.id: 0 for s in self.steps}
        graph: Dict[str, List[str]] = defaultdict(list)

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
