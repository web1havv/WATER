"""
WATER DataTransferManager — moves data between nodes after each step.

Supports three transfer modes:
  local  : simple shutil copy (same machine)
  ssh    : rsync over SSH (remote edge nodes, bandwidth-efficient)
  s3     : AWS S3 put/get (cloud storage backend)

Design principle: WATER abstracts transfer details so workflow authors
only declare WHAT moves (port mappings), not HOW it moves (protocol).
The DataTransferManager selects the optimal protocol based on:
  1. The declared transfer.protocol in the YAML
  2. Whether source and destination are on the same node
"""
from __future__ import annotations

import logging
import os
import shutil
import subprocess
from pathlib import Path
from typing import Optional

from water.registry.node_registry import Node
from water.schema.workflow import DataTransfer, TransferProtocol

logger = logging.getLogger("water.transfer")


class TransferError(Exception):
    pass


class DataTransferManager:
    """
    Executes data transfers between workflow step output and next step input.

    After step A completes on node-X, its outputs need to be visible to
    step B on node-Y as inputs. This manager bridges that gap.
    """

    def transfer(
        self,
        transfer: DataTransfer,
        src_node: Node,
        dst_node: Node,
        src_step_id: str,
        dst_step_id: str,
    ) -> None:
        """
        Execute a data transfer as declared in the workflow YAML.

        Args:
            transfer:    The DataTransfer definition from the workflow.
            src_node:    Node where the source step ran.
            dst_node:    Node where the destination step will run.
            src_step_id: Step ID that produced the data.
            dst_step_id: Step ID that will consume the data.
        """
        src_path = f"{src_node.data_root}/{src_step_id}/out/{transfer.from_port}"
        dst_path = f"{dst_node.data_root}/{dst_step_id}/in/{transfer.to_port}"

        # Auto-select local if same node, regardless of declared protocol
        effective_protocol = transfer.protocol
        if src_node.id == dst_node.id:
            effective_protocol = TransferProtocol.LOCAL

        logger.info(
            "Transferring %s -> %s  [%s]  %s -> %s",
            src_step_id, dst_step_id,
            effective_protocol.value,
            src_path, dst_path,
        )

        if effective_protocol == TransferProtocol.LOCAL:
            self._local_copy(src_path, dst_path)
        elif effective_protocol in (TransferProtocol.SSH, TransferProtocol.SFTP):
            self._rsync_ssh(src_path, dst_path, src_node, dst_node)
        elif effective_protocol == TransferProtocol.S3:
            self._s3_transfer(src_path, dst_path, dst_node)
        elif effective_protocol == TransferProtocol.NFS:
            # NFS: paths are already shared — dst just needs symlink / path update
            self._nfs_link(src_path, dst_path)
        else:
            raise TransferError(f"Unsupported protocol: {effective_protocol}")

    # ── Transfer implementations ─────────────────────────────────────

    def _local_copy(self, src: str, dst: str) -> None:
        """Copy within the same machine using shutil."""
        src_p = Path(src)
        dst_p = Path(dst)
        dst_p.parent.mkdir(parents=True, exist_ok=True)

        if src_p.is_dir():
            if dst_p.exists():
                shutil.rmtree(dst_p)
            shutil.copytree(src_p, dst_p)
        elif src_p.is_file():
            shutil.copy2(src_p, dst_p)
        else:
            logger.warning("Source path does not exist locally: %s", src)
            return

        logger.debug("Local copy complete: %s -> %s", src, dst)

    def _rsync_ssh(
        self, src: str, dst: str, src_node: Node, dst_node: Node
    ) -> None:
        """
        Transfer using rsync over SSH.

        rsync is preferred over scp because:
        - Incremental sync (only changed files transferred)
        - Bandwidth compression (-z)
        - Progress visibility
        - Handles large DICOM archives efficiently

        For healthcare DICOM data (often 10s of GBs), rsync's
        delta-transfer algorithm reduces transfer time dramatically
        on repeated runs.
        """
        key_arg = ["-e", f"ssh -i {dst_node.ssh_key_path} -p {dst_node.ssh_port}"] \
            if dst_node.ssh_key_path else ["-e", f"ssh -p {dst_node.ssh_port}"]

        # Ensure destination directory exists on remote
        mkdir_cmd = [
            "ssh",
            "-p", str(dst_node.ssh_port),
            f"{dst_node.ssh_user}@{dst_node.hostname}",
            f"mkdir -p {os.path.dirname(dst)}",
        ]
        if dst_node.ssh_key_path:
            mkdir_cmd.insert(1, "-i")
            mkdir_cmd.insert(2, dst_node.ssh_key_path)

        self._run(mkdir_cmd, "SSH mkdir")

        rsync_cmd = [
            "rsync",
            "-avz",          # archive, verbose, compress
            "--progress",
            *key_arg,
            src if src.endswith("/") else src + "/",
            f"{dst_node.ssh_user}@{dst_node.hostname}:{dst}",
        ]
        self._run(rsync_cmd, "rsync-ssh")

    def _s3_transfer(self, src: str, dst: str, dst_node: Node) -> None:
        """
        Upload to S3 bucket.
        Requires AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY in environment.
        For healthcare workflows, use a HIPAA-compliant S3 bucket with
        server-side encryption (SSE-S3 or SSE-KMS).
        """
        bucket = dst_node.label_dict().get("s3_bucket", "water-data")
        s3_dst = f"s3://{bucket}/{dst.lstrip('/')}"
        cmd = ["aws", "s3", "cp", "--recursive", src, s3_dst]
        self._run(cmd, "s3-upload")

    def _nfs_link(self, src: str, dst: str) -> None:
        """
        NFS: both nodes share the same filesystem mount.
        Just create a symlink so the destination step can read it.
        """
        dst_p = Path(dst)
        dst_p.parent.mkdir(parents=True, exist_ok=True)
        if dst_p.exists() or dst_p.is_symlink():
            dst_p.unlink()
        dst_p.symlink_to(src)
        logger.debug("NFS symlink: %s -> %s", dst, src)

    def _run(self, cmd: list[str], label: str) -> None:
        """Execute a shell command, raise TransferError on failure."""
        logger.debug("[%s] %s", label, " ".join(cmd))
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise TransferError(
                f"{label} failed (rc={result.returncode}): {result.stderr[:300]}"
            )


# ── Standalone test (no SSH/S3 needed — tests local mode) ──────────

def _test_local_transfer():
    import tempfile, os

    print("Testing local transfer...")
    with tempfile.TemporaryDirectory() as tmp:
        src = os.path.join(tmp, "step_a/out/dicom_files")
        dst = os.path.join(tmp, "step_b/in/dicom_files")
        os.makedirs(src)

        # Create fake DICOM files
        for i in range(5):
            with open(f"{src}/patient_{i:03d}.dcm", "w") as f:
                f.write(f"FAKE_DICOM_{i}")

        print(f"  Source files: {os.listdir(src)}")

        mgr = DataTransferManager()
        from water.schema.workflow import DataTransfer, TransferProtocol
        t = DataTransfer(
            from_step="step_a",
            from_port="dicom_files",
            to_step="step_b",
            to_port="dicom_files",
            protocol=TransferProtocol.LOCAL,
        )

        from water.registry.node_registry import Node
        # Use tmp as data_root so paths resolve correctly in the test
        same_node = Node(id="local-01", hostname="localhost", node_type="local", data_root=tmp)
        mgr.transfer(t, same_node, same_node, "step_a", "step_b")

        transferred = os.listdir(dst)
        assert len(transferred) == 5, f"Expected 5 files, got {len(transferred)}"
        print(f"  Destination files: {sorted(transferred)}")
        print("  LOCAL TRANSFER: PASS ✓")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    _test_local_transfer()
