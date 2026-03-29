"""
WATER NodeAgent — lightweight daemon that runs on each compute node.

Why this matters
----------------
The mentor explicitly described WATER as a "cluster farm" with "east/west
communication across inter-domain clusters."  That architecture requires each
node to be independently capable of:

  1. Advertising its live telemetry to the controller.
  2. Receiving and executing a workflow step when allocated.
  3. Reporting back completion status and freeing its resources.

The NodeAgent is that daemon.  It runs one per compute node (edge or cloud)
and communicates with the WATER controller over HTTP.  The controller never
needs to SSH into a node; instead it calls the NodeAgent's REST endpoints.
This naturally solves the NAT/firewall problem (nodes initiate outbound
connections) that several applicants raised in the discussions.

Design
------
  - Uses psutil for live CPU / RAM / GPU telemetry.
  - Registers itself with the controller on startup.
  - Sends heartbeat telemetry every N seconds (configurable).
  - Exposes a /run endpoint that accepts a step payload and launches the
    appropriate OCI container via docker-py.
  - Reports execution result back to the controller's /callbacks endpoint.

This is intentionally minimal — the NodeAgent is a thin execution shim,
not a scheduler.  All allocation intelligence stays in the WATER controller.
"""
from __future__ import annotations

import json
import logging
import os
import platform
import subprocess
import threading
import time
from typing import Any, Dict, Optional

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Telemetry helpers
# ---------------------------------------------------------------------------

def _cpu_cores() -> int:
    try:
        import psutil
        return psutil.cpu_count(logical=False) or 1
    except ImportError:
        return os.cpu_count() or 1


def _memory_gb() -> float:
    try:
        import psutil
        return round(psutil.virtual_memory().total / (1024 ** 3), 1)
    except ImportError:
        return 0.0


def _cpu_percent() -> float:
    try:
        import psutil
        return psutil.cpu_percent(interval=0.1)
    except ImportError:
        return 0.0


def _memory_used_percent() -> float:
    try:
        import psutil
        return psutil.virtual_memory().percent
    except ImportError:
        return 0.0


def _gpu_count() -> int:
    """Return number of available NVIDIA GPUs (0 if none or nvidia-smi missing)."""
    try:
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=name", "--format=csv,noheader"],
            capture_output=True, text=True, timeout=3,
        )
        if result.returncode == 0:
            lines = [l for l in result.stdout.strip().splitlines() if l]
            return len(lines)
    except Exception:
        pass
    return 0


def collect_telemetry(node_id: str, labels: Dict[str, str]) -> Dict[str, Any]:
    """Build a telemetry payload for the controller heartbeat."""
    return {
        "node_id":          node_id,
        "hostname":         platform.node(),
        "cpu_cores":        _cpu_cores(),
        "memory_gb":        _memory_gb(),
        "gpu_count":        _gpu_count(),
        "cpu_percent":      _cpu_percent(),
        "memory_percent":   _memory_used_percent(),
        "labels":           labels,
        "timestamp":        time.time(),
    }


# ---------------------------------------------------------------------------
# Step executor
# ---------------------------------------------------------------------------

class StepExecutor:
    """
    Runs a single workflow step on the local node using Docker.

    Accepts the same step payload as the WATER execution engine so the
    controller can dispatch to either local execution or a remote NodeAgent
    transparently.
    """

    def run(self, step_payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a step and return a result dict with exit_code and logs.

        step_payload keys (mirrors WorkflowStep):
          image, command, env, inputs_path, outputs_path
        """
        image   = step_payload.get("image", "")
        command = step_payload.get("command") or []
        env     = step_payload.get("env", {})
        inputs  = step_payload.get("inputs_path", "")
        outputs = step_payload.get("outputs_path", "")

        docker_cmd = ["docker", "run", "--rm"]

        for k, v in env.items():
            docker_cmd += ["-e", f"{k}={v}"]

        if inputs:
            docker_cmd += ["-v", f"{inputs}:/in:ro"]
        if outputs:
            os.makedirs(outputs, exist_ok=True)
            docker_cmd += ["-v", f"{outputs}:/out"]

        docker_cmd.append(image)
        docker_cmd.extend(command)

        log.info("NodeAgent executing: %s", " ".join(docker_cmd))
        try:
            result = subprocess.run(
                docker_cmd,
                capture_output=True,
                text=True,
                timeout=step_payload.get("timeout_seconds", 3600),
            )
            return {
                "exit_code": result.returncode,
                "stdout":    result.stdout[-4000:],
                "stderr":    result.stderr[-2000:],
                "status":    "completed" if result.returncode == 0 else "failed",
            }
        except subprocess.TimeoutExpired:
            return {"exit_code": -1, "status": "timeout", "stdout": "", "stderr": ""}
        except Exception as exc:
            return {"exit_code": -1, "status": "error", "stdout": "", "stderr": str(exc)}


# ---------------------------------------------------------------------------
# NodeAgent
# ---------------------------------------------------------------------------

class NodeAgent:
    """
    Lightweight daemon that runs on a compute node and bridges it into WATER.

    Responsibilities:
      - Register with the WATER controller on startup.
      - Send periodic heartbeat telemetry (CPU, RAM, GPU utilisation).
      - Accept step execution requests from the controller.
      - Report completion back to the controller's callback endpoint.

    The agent uses outbound HTTP connections only, so it works behind NAT
    and hospital firewalls without requiring inbound port exposure.

    Example usage (run on an edge node)::

        agent = NodeAgent(
            node_id="edge-pacs-01",
            controller_url="https://water-controller.hospital.net:8000",
            labels={"zone": "local", "type": "edge", "region": "alaska"},
            heartbeat_interval=15,
        )
        agent.start()   # blocks; ctrl-c to stop
    """

    def __init__(
        self,
        node_id: str,
        controller_url: str,
        labels: Optional[Dict[str, str]] = None,
        heartbeat_interval: int = 15,
    ) -> None:
        self.node_id            = node_id
        self.controller_url     = controller_url.rstrip("/")
        self.labels             = labels or {}
        self.heartbeat_interval = heartbeat_interval
        self._stop              = threading.Event()
        self._executor          = StepExecutor()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Register with the controller, then start the heartbeat loop."""
        self._register()
        log.info("NodeAgent '%s' started — controller: %s", self.node_id, self.controller_url)
        try:
            self._heartbeat_loop()
        except KeyboardInterrupt:
            log.info("NodeAgent '%s' stopping.", self.node_id)
            self._stop.set()

    def stop(self) -> None:
        self._stop.set()

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def _register(self) -> None:
        """POST node metadata to the controller's /nodes endpoint."""
        payload = {
            "id":        self.node_id,
            "hostname":  platform.node(),
            "cpu_cores": _cpu_cores(),
            "memory_gb": _memory_gb(),
            "gpu_count": _gpu_count(),
            "labels":    ",".join(f"{k}={v}" for k, v in self.labels.items()),
            "status":    "available",
        }
        self._post("/nodes", payload, label="register")

    # ------------------------------------------------------------------
    # Heartbeat
    # ------------------------------------------------------------------

    def _heartbeat_loop(self) -> None:
        """Send live telemetry to the controller every heartbeat_interval seconds."""
        while not self._stop.is_set():
            telemetry = collect_telemetry(self.node_id, self.labels)
            self._post(f"/nodes/{self.node_id}/heartbeat", telemetry, label="heartbeat")
            self._stop.wait(timeout=self.heartbeat_interval)

    # ------------------------------------------------------------------
    # Step execution (called by the controller)
    # ------------------------------------------------------------------

    def execute_step(self, step_payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a workflow step locally and return the result.

        In the full implementation the controller calls this via the agent's
        /run HTTP endpoint.  Here it is exposed as a method so tests can
        exercise execution directly.
        """
        result = self._executor.run(step_payload)
        result["node_id"] = self.node_id
        result["step_id"] = step_payload.get("step_id", "unknown")

        callback_url = step_payload.get("callback_url")
        if callback_url:
            self._post_absolute(callback_url, result, label="callback")

        return result

    # ------------------------------------------------------------------
    # HTTP helpers (pure stdlib — no extra dependencies)
    # ------------------------------------------------------------------

    def _post(self, path: str, payload: Dict[str, Any], label: str = "") -> None:
        self._post_absolute(self.controller_url + path, payload, label)

    @staticmethod
    def _post_absolute(url: str, payload: Dict[str, Any], label: str = "") -> None:
        import urllib.request
        data = json.dumps(payload).encode()
        req  = urllib.request.Request(
            url, data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                log.debug("[%s] %s → %s", label, url, resp.status)
        except Exception as exc:
            log.warning("[%s] POST to %s failed: %s", label, url, exc)


# ---------------------------------------------------------------------------
# Telemetry snapshot  (standalone utility — no controller needed)
# ---------------------------------------------------------------------------

def snapshot(node_id: str = "local", labels: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """Return a one-shot telemetry snapshot for this machine."""
    return collect_telemetry(node_id, labels or {})


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)

    if len(sys.argv) >= 3:
        agent = NodeAgent(
            node_id=sys.argv[1],
            controller_url=sys.argv[2],
            labels=dict(kv.split("=", 1) for kv in sys.argv[3:] if "=" in kv),
        )
        agent.start()
    else:
        # Just print telemetry for this machine
        import pprint
        pprint.pprint(snapshot())
