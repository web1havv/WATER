"""
WATER ExecutionEngine — drives workflow execution across nodes.

The engine:
  1. Resolves execution order via topological sort (WaterWorkflow.execution_order)
  2. Asks the Scheduler for a node per step
  3. Transfers input data to the target node (SSH/local)
  4. Runs the Docker container on the target node via SSH
  5. Transfers output data back (or leaves it on the node for the next step)
  6. Handles retries and status tracking
"""
from __future__ import annotations

import logging
import subprocess
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from water.registry.node_registry import Node, NodeRegistry
from water.scheduler.scheduler import Scheduler, SchedulerError
from water.schema.workflow import StepStatus, WaterWorkflow, WorkflowStep

logger = logging.getLogger("water.engine")


@dataclass
class StepResult:
    step_id: str
    node_id: str
    status: StepStatus
    exit_code: Optional[int] = None
    duration_seconds: float = 0.0
    error: Optional[str] = None


@dataclass
class WorkflowRun:
    workflow_name: str
    started_at: float = field(default_factory=time.time)
    results: List[StepResult] = field(default_factory=list)
    status: StepStatus = StepStatus.PENDING

    def is_successful(self) -> bool:
        return all(r.status == StepStatus.COMPLETED for r in self.results)

    def summary(self) -> str:
        lines = [f"Workflow: {self.workflow_name}", f"Status: {self.status.value}"]
        for r in self.results:
            lines.append(
                f"  [{r.status.value:10s}] {r.step_id} on {r.node_id} "
                f"({r.duration_seconds:.1f}s)"
            )
        return "\n".join(lines)


class ExecutionEngine:
    """
    Orchestrates a WaterWorkflow end-to-end.

    Each step is executed as:
      docker run --rm \\
        -v <data_root>/<step_id>/in:/in:ro \\
        -v <data_root>/<step_id>/out:/out \\
        <image> [command]

    For remote nodes, this command is wrapped in an SSH call.
    Data is transferred between steps using rsync over SSH.
    """

    def __init__(self, registry: NodeRegistry) -> None:
        self.registry = registry
        self.scheduler = Scheduler(registry)

    def run(self, workflow: WaterWorkflow) -> WorkflowRun:
        run = WorkflowRun(workflow_name=workflow.name)
        run.status = StepStatus.RUNNING

        waves = workflow.execution_order()
        logger.info("Execution plan: %s waves", len(waves))

        for wave_idx, wave in enumerate(waves):
            logger.info("Wave %d: %s", wave_idx + 1, wave)
            wave_results = []
            for step_id in wave:
                step = workflow.get_step(step_id)
                if step is None:
                    logger.error("Step %s not found in workflow", step_id)
                    continue
                result = self._execute_step(step, workflow, run)
                run.results.append(result)
                wave_results.append(result)

            if any(r.status == StepStatus.FAILED for r in wave_results):
                run.status = StepStatus.FAILED
                logger.error("Wave %d had failures; aborting.", wave_idx + 1)
                return run

        run.status = (
            StepStatus.COMPLETED if run.is_successful() else StepStatus.FAILED
        )
        return run

    def _execute_step(
        self, step: WorkflowStep, workflow: WaterWorkflow, run: WorkflowRun
    ) -> StepResult:
        try:
            node = self.scheduler.assign(step)
        except SchedulerError as exc:
            return StepResult(
                step_id=step.id,
                node_id="unassigned",
                status=StepStatus.FAILED,
                error=str(exc),
            )

        logger.info("Assigned step '%s' -> node '%s'", step.id, node.id)

        for attempt in range(step.retry_count + 1):
            result = self._run_on_node(step, node, workflow)
            if result.status == StepStatus.COMPLETED:
                return result
            logger.warning(
                "Step '%s' attempt %d/%d failed: %s",
                step.id, attempt + 1, step.retry_count + 1, result.error
            )
            time.sleep(2 ** attempt)

        return StepResult(
            step_id=step.id,
            node_id=node.id,
            status=StepStatus.FAILED,
            error=f"All {step.retry_count + 1} attempts failed",
        )

    def _run_on_node(
        self, step: WorkflowStep, node: Node, workflow: WaterWorkflow
    ) -> StepResult:
        start = time.time()

        env_vars = {**workflow.global_env, **step.env}
        env_flags = " ".join(f"-e {k}={v}" for k, v in env_vars.items())

        in_mount = f"{node.data_root}/{step.id}/in"
        out_mount = f"{node.data_root}/{step.id}/out"

        cmd_override = " ".join(step.command) if step.command else ""
        docker_cmd = (
            f"mkdir -p {in_mount} {out_mount} && "
            f"docker run --rm "
            f"-v {in_mount}:/in:ro "
            f"-v {out_mount}:/out "
            f"{env_flags} "
            f"{step.image} {cmd_override}"
        ).strip()

        if node.node_type == "local":
            full_cmd = ["bash", "-c", docker_cmd]
        else:
            ssh_target = f"{node.ssh_user}@{node.hostname}"
            key_arg = f"-i {node.ssh_key_path}" if node.ssh_key_path else ""
            full_cmd = [
                "ssh", "-p", str(node.ssh_port), key_arg,
                "-o", "StrictHostKeyChecking=no",
                ssh_target, docker_cmd,
            ]
            full_cmd = [arg for arg in full_cmd if arg]

        logger.debug("Running: %s", " ".join(full_cmd))

        try:
            proc = subprocess.run(
                full_cmd,
                capture_output=True,
                text=True,
                timeout=step.timeout_seconds or 3600,
            )
            duration = time.time() - start
            if proc.returncode == 0:
                return StepResult(
                    step_id=step.id,
                    node_id=node.id,
                    status=StepStatus.COMPLETED,
                    exit_code=0,
                    duration_seconds=duration,
                )
            else:
                return StepResult(
                    step_id=step.id,
                    node_id=node.id,
                    status=StepStatus.FAILED,
                    exit_code=proc.returncode,
                    duration_seconds=duration,
                    error=proc.stderr[:500],
                )
        except subprocess.TimeoutExpired:
            return StepResult(
                step_id=step.id,
                node_id=node.id,
                status=StepStatus.FAILED,
                error=f"Step timed out after {step.timeout_seconds}s",
            )
        except Exception as exc:
            return StepResult(
                step_id=step.id,
                node_id=node.id,
                status=StepStatus.FAILED,
                error=str(exc),
            )
