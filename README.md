# WATER: Workflow Allocation Towards Edge Resources

A distributed workflow orchestration framework for healthcare pipelines across edge and cloud nodes.

WATER lets you run multi-step healthcare data pipelines — like those built with [Niffler](https://github.com/Emory-HITI/Niffler) and [CONTROL-CORE](https://github.com/ControlCore-Project/) — across heterogeneous compute nodes without changing a single line of pipeline code.

## The Problem

Niffler's `modules/workflows/workflow.py` runs the entire DICOM processing pipeline sequentially on one machine. The README itself notes it "is currently causing issues by messing with the flow of other modules." When a hospital like the Alaska Native Medical Center processes 115,000+ imaging procedures per year from sites up to 720 miles away, a single-machine bottleneck is not viable.

WATER solves this by:
- Routing `cold-extraction` to the node closest to the PACS gateway
- Routing `png-extraction` to a GPU-capable edge node
- Routing `meta-extraction` to a cloud node with MongoDB
- Moving data between nodes automatically via rsync/SSH

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    WATER Control Plane                  │
│          FastAPI REST API  +  SQLite Node Registry      │
└──────────────┬──────────────────────────────────────────┘
               │
       ┌───────▼────────┐
       │   Scheduler    │  GPU-aware, label-based node selection
       └───────┬────────┘
               │
  ┌────────────▼───────────────────────────────────────┐
  │              Execution Engine                      │
  │  Topological sort → wave-parallel execution        │
  │  SSH + Docker per step, rsync for data transfer    │
  └────────────────────────────────────────────────────┘
               │
  ┌────────────▼────────────────────────────────────┐
  │           Edge / Cloud Nodes                    │
  │  edge-pacs-01   gpu-worker-01   cloud-indexer   │
  └─────────────────────────────────────────────────┘
```

## Quick Start

```bash
pip install -e .

# Register nodes
water nodes add --id edge-pacs-01 --host 192.168.1.10 --labels "role=pacs-gateway" --cpu 4 --mem 8.0
water nodes add --id gpu-01 --host 192.168.1.20 --labels "role=gpu-worker" --cpu 16 --mem 32.0 --gpu 2
water nodes ls

# Submit a workflow
water submit examples/niffler_dicom_pipeline.yaml

# Convert a concore GraphML study to WATER YAML
water convert path/to/study.graphml output.yaml
```

## Project Structure

```
water/
  schema/           Pydantic workflow models (YAML definitions)
  registry/         SQLite-backed node registry with heartbeat
  scheduler/        GPU-aware, label-selector step scheduler
  engine/
    execution_engine.py   Core orchestrator (SSH + Docker)
    data_transfer.py      local / rsync-SSH / S3 / NFS transfer
    parallel_executor.py  Wave-parallel step execution
    health_daemon.py      Background node liveness checker
    run_store.py          Persistent SQLite run history
  api/              FastAPI REST control plane
  cli.py            `water` CLI (nodes, submit, convert, status)
  adapters/         Niffler native adapter (auto-generate workflows)
  converters/       concore GraphML → WATER YAML converter
examples/
  niffler_dicom_pipeline.yaml   Full Niffler PNG pipeline
tests/              31 unit tests (pytest)
```

## Workflow Definition

Workflows are plain YAML files:

```yaml
name: niffler-dicom-pipeline
steps:
  - id: cold-extraction
    image: niffler/cold-extraction:latest
    node_selector: "role=pacs-gateway"
    outputs:
      - name: dicom_files
        path: cold_extraction/

  - id: png-extraction
    image: niffler/png-extraction:latest
    node_selector: "role=gpu-worker"
    depends_on: [cold-extraction]
    inputs:
      - name: dicom_files
        path: cold_extraction/CT/

transfers:
  - from_step: cold-extraction
    from_port: dicom_files
    to_step: png-extraction
    to_port: dicom_files
    protocol: ssh
```

## REST API

```bash
uvicorn water.api.app:app --reload

# Register a node
curl -X POST http://localhost:8000/nodes \
  -H "Content-Type: application/json" \
  -d '{"id":"edge-01","hostname":"10.0.1.10","labels":"role=pacs-gateway","cpu_cores":4}'

# Submit a workflow
curl -X POST http://localhost:8000/workflows/submit \
  -H "Content-Type: application/json" \
  -d '{"yaml_path":"examples/niffler_dicom_pipeline.yaml"}'

# Check cluster metrics
curl http://localhost:8000/metrics
```

## concore Integration

WATER can import existing CONTROL-CORE studies directly:

```bash
water convert path/to/study.graphml water_workflow.yaml
```

This converts a concore GraphML (controller ↔ physiological model loop) into a WATER workflow where each node runs in a Docker container on an assigned edge resource.

## Tests

```bash
pytest tests/ -v
# 31 tests passing
```

## Related Projects

- [Niffler](https://github.com/Emory-HITI/Niffler) — DICOM retrieval and processing framework
- [CONTROL-CORE](https://github.com/ControlCore-Project/) — Closed-loop neuromodulation control systems
- [WATER (upstream)](https://github.com/healthyinc/WATER) — Original project repository
