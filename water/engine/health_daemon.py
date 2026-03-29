"""
WATER HealthDaemon — background thread that pings each edge node
and automatically marks nodes OFFLINE when they stop responding.

This is critical for distributed healthcare workflows:
  - If a PACS-gateway node goes down mid-extraction, WATER must know
    immediately and re-schedule the step to another available node.
  - The Scheduler will never assign work to an OFFLINE node.

The daemon runs as a background thread alongside the FastAPI server.
It pings each online node via SSH's `exit 0` (cheapest possible probe)
and updates the registry's status + last_heartbeat on each cycle.

Usage:
    from water.engine.health_daemon import HealthDaemon
    daemon = HealthDaemon(registry)
    daemon.start()   # non-blocking
    # ... app runs ...
    daemon.stop()
"""
from __future__ import annotations

import logging
import subprocess
import threading
import time
from typing import Optional

from water.registry.node_registry import Node, NodeRegistry, NodeStatus

logger = logging.getLogger("water.health")


class HealthDaemon:
    """
    Background thread that health-checks all registered nodes every `interval` seconds.

    For each online node:
      1. Send `ssh <node> exit 0`   — cheapest liveness probe
      2. If it succeeds within TIMEOUT: mark ONLINE, record heartbeat
      3. If it fails for FAILURE_THRESHOLD consecutive checks: mark OFFLINE

    For each offline node:
      1. Try the same probe — if it responds, mark ONLINE again (auto-recovery)
    """

    PROBE_TIMEOUT   = 5    # seconds per SSH probe
    FAILURE_THRESHOLD = 3  # consecutive failures before marking OFFLINE

    def __init__(
        self,
        registry: NodeRegistry,
        interval: float = 30.0,
    ) -> None:
        self.registry = registry
        self.interval = interval
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._failure_counts: dict[str, int] = {}

    def start(self) -> None:
        """Start the health-check daemon in a background thread."""
        self._thread = threading.Thread(
            target=self._run_loop,
            name="water-health-daemon",
            daemon=True,
        )
        self._thread.start()
        logger.info("HealthDaemon started (interval=%ds)", self.interval)

    def stop(self) -> None:
        """Signal the daemon to stop and wait for it to finish."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=10)
        logger.info("HealthDaemon stopped")

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._check_all_nodes()
            except Exception as exc:
                logger.error("Health check error: %s", exc)
            self._stop_event.wait(timeout=self.interval)

    def _check_all_nodes(self) -> None:
        nodes = self.registry.list_all()
        for node in nodes:
            if node.node_type == "local":
                # Local nodes are always considered healthy
                self.registry.heartbeat(node.id)
                continue
            alive = self._probe(node)
            self._update_status(node, alive)

    def _probe(self, node: Node) -> bool:
        """
        Probe a remote node via SSH.
        Returns True if the node responds within PROBE_TIMEOUT seconds.

        SSH exit 0 is the lightest possible liveness check:
        - No file transfer
        - No remote command execution overhead
        - Fails fast on network partition or host down
        """
        key_args = ["-i", node.ssh_key_path] if node.ssh_key_path else []
        cmd = [
            "ssh",
            "-o", "ConnectTimeout=5",
            "-o", "StrictHostKeyChecking=no",
            "-o", "BatchMode=yes",    # never prompt for password
            "-p", str(node.ssh_port),
            *key_args,
            f"{node.ssh_user}@{node.hostname}",
            "exit 0",
        ]
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                timeout=self.PROBE_TIMEOUT,
            )
            return result.returncode == 0
        except (subprocess.TimeoutExpired, OSError):
            return False

    def _update_status(self, node: Node, alive: bool) -> None:
        node_id = node.id
        if alive:
            self._failure_counts[node_id] = 0
            if node.status != NodeStatus.ONLINE:
                logger.info("Node '%s' recovered -> ONLINE", node_id)
                self.registry.update_status(node_id, NodeStatus.ONLINE)
            self.registry.heartbeat(node_id)
        else:
            count = self._failure_counts.get(node_id, 0) + 1
            self._failure_counts[node_id] = count
            logger.warning(
                "Node '%s' probe failed (%d/%d)",
                node_id, count, self.FAILURE_THRESHOLD
            )
            if count >= self.FAILURE_THRESHOLD and node.status == NodeStatus.ONLINE:
                logger.error(
                    "Node '%s' marked OFFLINE after %d failures",
                    node_id, count
                )
                self.registry.update_status(node_id, NodeStatus.OFFLINE)


# ── Standalone demo (runs a simulated health cycle) ──────────────────
def _demo():
    import tempfile
    from pathlib import Path
    from water.registry.node_registry import Node, NodeRegistry

    with tempfile.TemporaryDirectory() as tmp:
        registry = NodeRegistry(db_path=Path(tmp) / "health_demo.db")
        registry.register(Node(
            id="local-sim",
            hostname="localhost",
            node_type="local",
        ))

        daemon = HealthDaemon(registry, interval=2.0)
        daemon.start()

        print("HealthDaemon running... (2 cycles)")
        time.sleep(4.5)
        daemon.stop()

        node = registry.get("local-sim")
        print(f"Node status: {node.status}")
        print(f"Last heartbeat: {node.last_heartbeat:.2f}")
        assert node.status == NodeStatus.ONLINE, "Local node should be online"
        print("HealthDaemon demo: PASS ✓")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    _demo()
