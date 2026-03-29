# WATER: Workflow Allocation Towards Edge Resources

An allocation and execution fabric for healthcare workflows across edge and cloud nodes.

WATER is **not** a healthcare pipeline — it is the layer *beneath* healthcare pipelines.  It decides *where* a containerised workload runs across a cluster farm of edge and cloud nodes, enforces privacy constraints (including HIPAA-aligned data-locality), and moves data between nodes.  What runs inside the containers is none of WATER's business.

---

## The Problem

Modern healthcare AI involves multiple organisations, multiple compute environments, and strict data-sovereignty requirements.  A clinical imaging AI might need:

- **Data ingest** on a hospital edge node (data must not leave the premises)
- **Preprocessing** on a GPU-capable edge server
- **Training / inference** on a cloud node with more compute
- **Results aggregated** back at the hospital

No existing lightweight framework handles this *allocation* problem in a platform-agnostic way.  Existing tools either run everything on one machine (Niffler, CONTROL-CORE) or require a full Kubernetes cluster (overkill for a two-hospital setup).  WATER fills the gap.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                      WATER Controller                            │
│                                                                  │
│   REST API (FastAPI)  ·  Allocation Engine  ·  Run Store (SQLite)│
│                                                                  │
│        ┌──────────────────────────────────────┐                  │
│        │         Pluggable Scheduler          │                  │
│        │  PrivacyFirstPolicy                  │                  │
│        │  LatencyOptimizedPolicy              │                  │
│        │  DefaultPolicy  (swappable)          │                  │
│        └──────────────────────────────────────┘                  │
└────────────────────┬─────────────────────────────────────────────┘
          East/West inter-domain communication
     ┌────────────────┼────────────────┐
     ▼                ▼                ▼
┌─────────┐    ┌─────────┐    ┌─────────────┐
│NodeAgent│    │NodeAgent│    │  NodeAgent  │
│site-A   │    │site-B   │    │  site-C     │
│(Alaska) │    │(Cloud)  │    │(Remote AK)  │
└────┬────┘    └────┬────┘    └──────┬──────┘
     │              │                │
  Docker          Docker           Docker
  (any OCI)      (any OCI)        (any OCI)
```

**Key design decisions:**

| Decision | Rationale |
|----------|-----------|
| Privacy as hard disqualifier | `strict-local` removes cloud nodes *before* scoring, not as a penalty |
| Pluggable scheduling policies | Swap `PrivacyFirstPolicy` ↔ `LatencyOptimizedPolicy` without touching the engine |
| NodeAgent per node | Nodes initiate outbound connections — works behind hospital NAT/firewalls |
| OCI containers only | WATER never touches the medical logic inside a container |
| YAML intent files | Researchers describe *what* they need; WATER decides *where* |

---

## Quick Start

```bash
pip install -e .

# Start the controller
uvicorn water.api.app:app --reload

# On each compute node, start a NodeAgent
python -m water.engine.node_agent edge-pacs-01 http://controller:8000 zone=local type=edge

# Submit a workflow
water submit examples/imaging_pipeline.yaml

# Or use the REST API
curl -X POST http://localhost:8000/workflows/submit \
  -H "Content-Type: application/json" \
  -d '{"yaml_path": "examples/federated_ml.yaml"}'
```

---

## Workflow Intent Files

Any healthcare workflow is described as a YAML intent file:

```yaml
name: my-pipeline
intent:
  privacy: strict-local   # hard constraint — cloud disqualified entirely
  latency: batch
  compute: gpu-required
  tags:
    project: radiology-ai
    region: alaska

steps:
  - id: preprocess
    image: myorg/preprocessor:1.0
    node_selector: "zone=local"
    outputs:
      - name: tensors
        path: out/tensors/

  - id: inference
    image: myorg/inference-gpu:1.0
    node_selector: "gpu=true"
    depends_on: [preprocess]
    inputs:
      - name: tensors
        path: out/tensors/

transfers:
  - from_step: preprocess
    from_port: tensors
    to_step: inference
    to_port: tensors
    protocol: ssh
```

WATER reads the intent, filters nodes by privacy level, scores remaining candidates, and dispatches each step to the optimal node.

---

## Privacy Enforcement

WATER enforces three privacy levels as **hard disqualifiers** (not scoring penalties):

| Level | Behaviour |
|-------|-----------|
| `strict-local` | Only `zone=local` nodes eligible. Cloud removed before scoring starts. |
| `edge-only` | Edge preferred; cloud used only as fallback. |
| `public` | No zone constraint — optimise for resources. |

This architecture was explicitly requested by the mentors: *"privacy is a hard disqualifier, not a scoring penalty."*

---

## Pluggable Scheduling Policies

```python
from water.scheduler.scheduler import Scheduler, get_policy
from water.registry.node_registry import NodeRegistry

registry = NodeRegistry()
scheduler = Scheduler(registry, policy=get_policy("privacy-first"))
node = scheduler.assign(step, intent)
```

Available policies: `privacy-first` · `latency-optimized` · `default`

Adding a new policy requires only implementing the `SchedulingPolicy` interface — no changes to the scheduler core:

```python
class MyPolicy(SchedulingPolicy):
    def filter(self, nodes, step, intent): ...
    def score(self, node, step, intent): ...
```

---

## NodeAgent

Each compute node runs a lightweight `NodeAgent` daemon that:
- Registers itself with the WATER controller on startup
- Sends live CPU / RAM / GPU telemetry every 15 seconds
- Accepts step execution requests and runs them as Docker containers
- Reports completion status back to the controller

Agents use **outbound-only HTTP** connections — no inbound ports needed. This handles hospital NAT/firewalls without VPN or port-forwarding.

```bash
# On an edge node
python -m water.engine.node_agent \
  edge-01 \
  http://water-controller.hospital.net:8000 \
  zone=local type=edge region=alaska
```

---

## Example Workflows

| File | Domain | Privacy | Nodes |
|------|--------|---------|-------|
| `examples/imaging_pipeline.yaml` | Medical imaging (AI inference) | `strict-local` | edge + GPU edge |
| `examples/genomics_pipeline.yaml` | WES/WGS variant calling | `public` | edge + cloud |
| `examples/federated_ml.yaml` | Federated learning across hospitals | `strict-local` | 3 hospital sites + coordinator |

These examples show that WATER is domain-agnostic.  The framework works identically whether the containers run Niffler, CONTROL-CORE, GATK, or a custom ML training job.

---

## Project Layout

```
water/
  schema/
    workflow.py           WorkflowIntent + WorkflowStep + DataTransfer (Pydantic v2)
  scheduler/
    scheduler.py          Pluggable policy engine: PrivacyFirst / LatencyOptimized / Default
  registry/
    node_registry.py      SQLite-backed node registry with label-selector support
  engine/
    node_agent.py         Per-node daemon: telemetry heartbeat + Docker step execution
    execution_engine.py   Controller-side orchestrator: SSH + Docker dispatch
    data_transfer.py      local / rsync-SSH / S3 / NFS inter-node transfer
    parallel_executor.py  Wave-parallel step execution via ThreadPoolExecutor
    health_daemon.py      Background node liveness checker (auto-OFFLINE after 3 failures)
    run_store.py          Persistent SQLite run history
  api/
    app.py                FastAPI REST control plane
  cli.py                  `water` CLI: nodes, submit, convert, status
  adapters/
    niffler.py            Example adapter: auto-generate workflows from Niffler configs
  converters/
    graphml_to_water.py   concore GraphML → WATER YAML converter
examples/
  imaging_pipeline.yaml   Medical imaging (strict-local, GPU)
  genomics_pipeline.yaml  Variant calling (public, cloud)
  federated_ml.yaml       Federated learning across hospital sites
tests/                    pytest suite (31 tests)
pyproject.toml            pip install -e . ready
```

---

## Tests

```bash
pytest tests/ -v
# 31 tests — schema, registry, scheduler (all policies)
```

---

## Related Work

- [Niffler](https://github.com/Emory-HITI/Niffler) — DICOM retrieval and processing (one of many workloads WATER can orchestrate)
- [CONTROL-CORE](https://github.com/ControlCore-Project/) — Closed-loop neuromodulation control (another example workload)
- [WATER upstream](https://github.com/healthyinc/WATER) — Original project repository
