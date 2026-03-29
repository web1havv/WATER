"""
WATER REST API — FastAPI application.

Endpoints:
  GET  /health                   Liveness check
  GET  /nodes                    List all registered nodes
  POST /nodes                    Register a new node
  DELETE /nodes/{node_id}        Remove a node
  POST /nodes/{node_id}/heartbeat  Update node heartbeat

  POST /workflows/submit         Submit a YAML workflow for execution
  GET  /workflows/{run_id}       Get run status and step results
  GET  /workflows/{run_id}/steps List per-step results

  GET  /metrics                  Cluster utilization summary

This API is the control plane for WATER. Operators use it to:
  - register/deregister edge nodes
  - submit healthcare workflow pipelines
  - monitor execution progress
"""
from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from water.engine.execution_engine import ExecutionEngine, WorkflowRun
from water.registry.node_registry import Node, NodeRegistry, NodeStatus
from water.schema.workflow import StepStatus, WaterWorkflow

# ── App setup ────────────────────────────────────────────────────────
app = FastAPI(
    title="WATER API",
    description="Workflow Allocation Towards Edge Resources — REST Control Plane",
    version="0.1.0",
)

DB_PATH = Path.home() / ".water" / "registry.db"
registry = NodeRegistry(db_path=DB_PATH)
engine = ExecutionEngine(registry)

# In-memory run store (Week 7+ will migrate to SQLite)
_runs: Dict[str, WorkflowRun] = {}


# ── Request/Response schemas ──────────────────────────────────────────

class NodeRegisterRequest(BaseModel):
    id: str
    hostname: str
    ssh_user: str = "water"
    ssh_port: int = 22
    ssh_key_path: Optional[str] = None
    node_type: str = "edge"
    labels: str = ""
    cpu_cores: int = 1
    memory_gb: float = 1.0
    gpu_count: int = 0
    data_root: str = "/tmp/water"


class NodeResponse(BaseModel):
    id: str
    hostname: str
    node_type: str
    labels: str
    cpu_cores: int
    memory_gb: float
    gpu_count: int
    status: str
    last_heartbeat: float


class StepResultResponse(BaseModel):
    step_id: str
    node_id: str
    status: str
    exit_code: Optional[int]
    duration_seconds: float
    error: Optional[str]


class RunResponse(BaseModel):
    run_id: str
    workflow_name: str
    status: str
    started_at: float
    steps: List[StepResultResponse]


class SubmitWorkflowRequest(BaseModel):
    yaml_content: str = ""
    yaml_path: Optional[str] = None


class MetricsResponse(BaseModel):
    total_nodes: int
    online_nodes: int
    gpu_nodes: int
    total_cpu_cores: int
    total_memory_gb: float
    active_runs: int


# ── Health ────────────────────────────────────────────────────────────

@app.get("/health", tags=["System"])
def health():
    return {"status": "ok", "version": "0.1.0"}


# ── Node management ───────────────────────────────────────────────────

@app.get("/nodes", response_model=List[NodeResponse], tags=["Nodes"])
def list_nodes():
    nodes = registry.list_all()
    return [_node_to_resp(n) for n in nodes]


@app.post("/nodes", response_model=NodeResponse, status_code=201, tags=["Nodes"])
def register_node(req: NodeRegisterRequest):
    node = Node(**req.model_dump())
    registry.register(node)
    return _node_to_resp(node)


@app.delete("/nodes/{node_id}", tags=["Nodes"])
def remove_node(node_id: str):
    node = registry.get(node_id)
    if not node:
        raise HTTPException(status_code=404, detail=f"Node '{node_id}' not found")
    registry.remove(node_id)
    return {"message": f"Node '{node_id}' removed"}


@app.post("/nodes/{node_id}/heartbeat", tags=["Nodes"])
def node_heartbeat(node_id: str):
    node = registry.get(node_id)
    if not node:
        raise HTTPException(status_code=404, detail=f"Node '{node_id}' not found")
    registry.heartbeat(node_id)
    return {"message": "heartbeat recorded"}


@app.patch("/nodes/{node_id}/status", tags=["Nodes"])
def update_node_status(node_id: str, status: str):
    node = registry.get(node_id)
    if not node:
        raise HTTPException(status_code=404, detail=f"Node '{node_id}' not found")
    try:
        s = NodeStatus(status)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid status '{status}'")
    registry.update_status(node_id, s)
    return {"message": f"Node '{node_id}' status set to '{status}'"}


# ── Workflow submission ───────────────────────────────────────────────

@app.post("/workflows/submit", response_model=RunResponse, status_code=202, tags=["Workflows"])
def submit_workflow(req: SubmitWorkflowRequest):
    """
    Submit a WATER workflow for execution.
    Accepts either inline YAML content or a path to a YAML file on the server.
    """
    try:
        if req.yaml_content:
            raw = yaml.safe_load(req.yaml_content)
        elif req.yaml_path:
            p = Path(req.yaml_path)
            if not p.exists():
                raise HTTPException(status_code=400, detail=f"YAML file not found: {req.yaml_path}")
            raw = yaml.safe_load(p.read_text())
        else:
            raise HTTPException(status_code=400, detail="Provide yaml_content or yaml_path")

        workflow = WaterWorkflow(**raw)
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Workflow parse error: {exc}")

    run_id = str(uuid.uuid4())[:8]
    run = engine.run(workflow)
    _runs[run_id] = run

    return _run_to_resp(run_id, run)


@app.get("/workflows/{run_id}", response_model=RunResponse, tags=["Workflows"])
def get_run(run_id: str):
    run = _runs.get(run_id)
    if not run:
        raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found")
    return _run_to_resp(run_id, run)


@app.get("/workflows/{run_id}/steps", response_model=List[StepResultResponse], tags=["Workflows"])
def get_run_steps(run_id: str):
    run = _runs.get(run_id)
    if not run:
        raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found")
    return [_step_to_resp(r) for r in run.results]


# ── Metrics ───────────────────────────────────────────────────────────

@app.get("/metrics", response_model=MetricsResponse, tags=["System"])
def metrics():
    all_nodes = registry.list_all()
    online = [n for n in all_nodes if n.status == NodeStatus.ONLINE]
    gpu_nodes = [n for n in online if n.gpu_count > 0]
    return MetricsResponse(
        total_nodes=len(all_nodes),
        online_nodes=len(online),
        gpu_nodes=len(gpu_nodes),
        total_cpu_cores=sum(n.cpu_cores for n in online),
        total_memory_gb=sum(n.memory_gb for n in online),
        active_runs=sum(1 for r in _runs.values() if r.status == StepStatus.RUNNING),
    )


# ── Helpers ───────────────────────────────────────────────────────────

def _node_to_resp(n: Node) -> NodeResponse:
    return NodeResponse(
        id=n.id,
        hostname=n.hostname,
        node_type=n.node_type,
        labels=n.labels,
        cpu_cores=n.cpu_cores,
        memory_gb=n.memory_gb,
        gpu_count=n.gpu_count,
        status=n.status,
        last_heartbeat=n.last_heartbeat,
    )


def _step_to_resp(r: Any) -> StepResultResponse:
    return StepResultResponse(
        step_id=r.step_id,
        node_id=r.node_id,
        status=r.status.value,
        exit_code=r.exit_code,
        duration_seconds=r.duration_seconds,
        error=r.error,
    )


def _run_to_resp(run_id: str, run: WorkflowRun) -> RunResponse:
    return RunResponse(
        run_id=run_id,
        workflow_name=run.workflow_name,
        status=run.status.value,
        started_at=run.started_at,
        steps=[_step_to_resp(r) for r in run.results],
    )
