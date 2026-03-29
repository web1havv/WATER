"""
WATER Niffler Adapter — native integration between Niffler modules and WATER.

Instead of manually writing Docker images and YAML steps for every Niffler module,
this adapter auto-generates a complete WATER workflow from a Niffler pipeline config.

It knows the exact config schema of every Niffler module (from reading the source):
  - cold-extraction : system.json + CSV file
  - meta-extraction : system.json + featureset.txt + MongoDB URI
  - png-extraction  : config.json (DICOMHome, OutputDirectory, Depth, ...)
  - dicom-anon      : input/output folders

Usage:
    from water.adapters.niffler import NifflerAdapter
    adapter = NifflerAdapter()
    workflow = adapter.png_pipeline(
        dicom_source="/data/dicom",
        output_root="/data/water_out",
        pacs_node="edge-pacs-01",
        gpu_node="gpu-worker-01",
    )
    yaml_str = workflow.model_dump_json()
"""
from __future__ import annotations

from typing import Optional

from water.schema.workflow import (
    DataPort, DataTransfer, TransferProtocol,
    WaterWorkflow, WorkflowStep,
)


class NifflerAdapter:
    """
    Generates WATER workflows from Niffler pipeline configurations.

    This adapter solves the exact limitations documented in Niffler's
    modules/workflows/README.md:
      "workflow.py... is currently causing issues by messing with the
       flow of other modules."

    By replacing workflow.py with WATER's engine, each step runs in
    an isolated container on the optimal node — no more os.chdir()
    pollution and no more single-machine bottleneck.
    """

    IMAGES = {
        "cold-extraction":    "niffler/cold-extraction:latest",
        "modality-grouping":  "niffler/workflows:latest",
        "png-extraction":     "niffler/png-extraction:latest",
        "meta-extraction":    "niffler/meta-extraction:latest",
        "dicom-anonymization": "niffler/dicom-anonymization:latest",
    }

    def png_pipeline(
        self,
        dicom_source: str,
        output_root: str,
        pacs_node: Optional[str] = None,
        gpu_node: Optional[str] = None,
        mongo_node: Optional[str] = None,
        depth: int = 3,
        use_gpu: bool = True,
        split_chunks: int = 4,
        send_email: bool = False,
    ) -> WaterWorkflow:
        """
        Build a WATER workflow mirroring Niffler's full PNG pipeline:
          cold-extraction -> modality-grouping -> png-extraction -> meta-extraction

        Args:
            dicom_source:  Path to input DICOM directory.
            output_root:   Base output directory (per-step subdirs created automatically).
            pacs_node:     node_selector for PACS-adjacent steps (cold, modality).
            gpu_node:      node_selector for CPU/GPU-heavy step (png).
            mongo_node:    node_selector for MongoDB-bound step (meta).
            depth:         Folder hierarchy depth in DICOMHome.
            use_gpu:       Whether png-extraction should request a GPU node.
            split_chunks:  SplitIntoChunks for png-extraction.
            send_email:    Whether to send completion email.
        """
        cold = WorkflowStep(
            id="cold-extraction",
            name="DICOM Cold Extraction (C-MOVE from PACS)",
            image=self.IMAGES["cold-extraction"],
            node_selector=pacs_node,
            inputs=[
                DataPort(name="csv_input", path="csv/patients.csv",
                         description="Patient CSV with PatientID, AccessionNumber")
            ],
            outputs=[
                DataPort(name="dicom_files", path="cold_extraction/",
                         description="Retrieved DICOMs in hierarchy")
            ],
            env={
                "STORAGE_FOLDER": f"{output_root}/cold_extraction",
                "NIGHTLY_ONLY": "false",
                "SEND_EMAIL": str(send_email).lower(),
            },
            retry_count=3,
            timeout_seconds=7200,
        )

        modality = WorkflowStep(
            id="modality-grouping",
            name="Sort DICOMs by Modality (CT, MRI, CR…)",
            image=self.IMAGES["modality-grouping"],
            node_selector=pacs_node,
            depends_on=["cold-extraction"],
            inputs=[DataPort(name="dicom_files", path="cold_extraction/")],
            outputs=[DataPort(name="sorted_dicoms", path="modality_grouping/")],
            env={
                "COLD_EXTRACTION_PATH": f"{output_root}/cold_extraction",
                "MODALITY_SPLIT_PATH": f"{output_root}/modality_grouping",
            },
            retry_count=1,
        )

        png = WorkflowStep(
            id="png-extraction",
            name="Convert DICOM to PNG + Extract Metadata CSV",
            image=self.IMAGES["png-extraction"],
            node_selector=gpu_node,
            depends_on=["modality-grouping"],
            inputs=[DataPort(name="sorted_dicoms", path="modality_grouping/CT/")],
            outputs=[
                DataPort(name="png_images", path="png_output/extracted-images/"),
                DataPort(name="metadata_csv", path="png_output/metadata.csv"),
            ],
            env={
                "DICOM_HOME": f"{output_root}/modality_grouping/CT",
                "OUTPUT_DIRECTORY": f"{output_root}/png_output",
                "DEPTH": str(depth),
                "PRINT_IMAGES": "true",
                "COMMON_HEADERS_ONLY": "true",
                "USE_GPU": str(use_gpu).lower(),
                "SPLIT_INTO_CHUNKS": str(split_chunks),
                "SEND_EMAIL": str(send_email).lower(),
            },
            retry_count=2,
            timeout_seconds=14400,
        )

        meta = WorkflowStep(
            id="meta-extraction",
            name="Real-time Metadata Indexing into MongoDB",
            image=self.IMAGES["meta-extraction"],
            node_selector=mongo_node,
            depends_on=["png-extraction"],
            inputs=[DataPort(name="metadata_csv", path="png_output/metadata.csv")],
            outputs=[DataPort(name="done_flag", path="meta_extraction/done.txt")],
            env={
                "MONGO_URI": "mongodb://localhost:27017/niffler",
                "FEATURES_FOLDER": "/src/conf",
                "SEND_EMAIL": str(send_email).lower(),
            },
            retry_count=2,
        )

        protocol = TransferProtocol.LOCAL if not gpu_node else TransferProtocol.SSH

        return WaterWorkflow(
            name="niffler-png-pipeline",
            version="1.0",
            description=(
                "Auto-generated WATER workflow wrapping Niffler's PNG extraction pipeline. "
                "Replaces workflow.py's sequential single-machine execution with "
                "distributed edge-aware step scheduling."
            ),
            steps=[cold, modality, png, meta],
            transfers=[
                DataTransfer(from_step="cold-extraction", from_port="dicom_files",
                             to_step="modality-grouping", to_port="dicom_files",
                             protocol=TransferProtocol.LOCAL),
                DataTransfer(from_step="modality-grouping", from_port="sorted_dicoms",
                             to_step="png-extraction", to_port="sorted_dicoms",
                             protocol=protocol),
                DataTransfer(from_step="png-extraction", from_port="metadata_csv",
                             to_step="meta-extraction", to_port="metadata_csv",
                             protocol=protocol),
            ],
            global_env={
                "NIFFLER_LOG_LEVEL": "INFO",
                "DICOM_SOURCE": dicom_source,
                "OUTPUT_ROOT": output_root,
            },
        )


def _demo():
    adapter = NifflerAdapter()
    wf = adapter.png_pipeline(
        dicom_source="/data/pacs",
        output_root="/data/water_out",
        pacs_node="role=pacs-gateway",
        gpu_node="role=gpu-worker",
        mongo_node="role=cloud-indexer",
    )
    print(f"Generated workflow: '{wf.name}'  ({len(wf.steps)} steps)")
    for step in wf.steps:
        print(f"  {step.id:25s} image={step.image}  selector='{step.node_selector}'")
    print(f"\nExecution waves: {wf.execution_order()}")
    print(f"Transfers: {len(wf.transfers)}")
    print("\nNiffler Adapter Demo: PASS ✓")


if __name__ == "__main__":
    import sys
    sys.path.insert(0, __file__.rsplit("/water/", 1)[0])
    _demo()
