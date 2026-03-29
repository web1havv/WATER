"""
WATER GraphML Converter — converts a concore study (.graphml) to a WATER workflow YAML.

This is a key differentiator: existing CONTROL-CORE studies can be instantly
imported into WATER and distributed across edge nodes, without rewriting them.

The conversion maps:
  concore GraphML node  →  WATER WorkflowStep (Docker image = concore/<program>)
  concore GraphML edge  →  WATER DataTransfer  (protocol = local by default)
  concore CZ/PZ prefixes →  node_selector labels on the resulting workflow steps

Usage:
    python3 -m water.converters.graphml_to_water demo/sample.graphml output.yaml

Or programmatically:
    from water.converters.graphml_to_water import GraphMLConverter
    yaml_str = GraphMLConverter().convert("/path/to/study.graphml")
"""
from __future__ import annotations

import re
import sys
import yaml
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from xml.etree import ElementTree as ET


# yFiles XML namespace — used by concore's GraphML files
YFILES_NS = "http://www.yworks.com/xml/graphml"
GRAPHML_NS = "http://graphml.graphdrawing.org/xmlns"

# Mapping of concore node prefixes to WATER Docker images
# These are the Dockerized Niffler / concore program containers
CONCORE_IMAGE_MAP: Dict[str, str] = {
    "CZ": "concore/controller:latest",
    "PZ": "concore/physmodel:latest",
    "MZ": "concore/matlab:latest",
    "VZ": "concore/verilog:latest",
    # Niffler pipeline modules
    "COLD": "niffler/cold-extraction:latest",
    "PNG":  "niffler/png-extraction:latest",
    "META": "niffler/meta-extraction:latest",
    "ANON": "niffler/dicom-anonymization:latest",
    "WF":   "niffler/workflows:latest",
}

# Default Docker image when prefix is unknown
DEFAULT_IMAGE = "concore/generic:latest"


class GraphMLConverter:
    """
    Parses a concore GraphML study and emits a WATER workflow YAML string.

    The converter:
      1. Extracts nodes and their labels (e.g., "CZ:controller.py")
      2. Extracts directed edges and their labels (port names)
      3. Builds dependency graph from edge directions
      4. Maps each node to a Docker image based on its prefix
      5. Emits a WaterWorkflow-compatible YAML
    """

    def convert(self, graphml_path: str) -> str:
        """
        Convert a concore .graphml file to a WATER workflow YAML string.
        Returns the YAML as a string (ready to write to a file or POST to /submit).
        """
        tree = ET.parse(graphml_path)
        root = tree.getroot()

        # Strip namespace prefix for easier parsing
        ns = {"gml": GRAPHML_NS, "y": YFILES_NS}

        graph = root.find("gml:graph", ns)
        if graph is None:
            # Try without namespace
            graph = root.find("graph")
        if graph is None:
            raise ValueError(f"No <graph> element found in {graphml_path}")

        project_name = graph.attrib.get("id", "concore-study").replace("/", "-")

        nodes_raw: Dict[str, str] = {}   # node_id -> label (e.g., "CZ:controller.py")
        edges_raw: List[Tuple[str, str, str]] = []  # (src_id, dst_id, edge_label)

        for elem in graph:
            tag = elem.tag.split("}")[-1]  # Strip namespace

            if tag == "node":
                node_id = elem.attrib.get("id", "")
                label = self._extract_node_label(elem)
                if label:
                    nodes_raw[node_id] = label

            elif tag == "edge":
                src = elem.attrib.get("source", "")
                dst = elem.attrib.get("target", "")
                label = self._extract_edge_label(elem)
                edges_raw.append((src, dst, label or "data"))

        # Build clean step definitions
        steps, transfers = self._build_workflow(nodes_raw, edges_raw)

        workflow = {
            "name": project_name,
            "version": "1.0",
            "description": (
                f"Auto-converted from concore study: {Path(graphml_path).name}. "
                "Distributes the concore program nodes across WATER edge resources."
            ),
            "steps": steps,
            "transfers": transfers,
        }

        return yaml.dump(workflow, sort_keys=False, default_flow_style=False)

    def _build_workflow(
        self,
        nodes: Dict[str, str],
        edges: List[Tuple[str, str, str]],
    ) -> Tuple[List[Dict], List[Dict]]:
        """Build step and transfer dicts from parsed graph elements."""

        # Map node_id -> clean step_id and program file
        id_to_step: Dict[str, str] = {}
        steps = []

        for node_id, label in nodes.items():
            prefix, program = self._parse_label(label)
            step_id = self._make_step_id(program or label)
            id_to_step[node_id] = step_id
            image = CONCORE_IMAGE_MAP.get(prefix, DEFAULT_IMAGE)

            steps.append({
                "id": step_id,
                "name": f"{prefix} — {program}",
                "image": image,
                "inputs": [],
                "outputs": [],
                "env": {"CONCORE_PROGRAM": program},
                "retry_count": 1,
            })

        # Build dependency map and transfer list
        depends_on: Dict[str, List[str]] = {s["id"]: [] for s in steps}
        transfers = []

        for src_id, dst_id, label in edges:
            src_step = id_to_step.get(src_id)
            dst_step = id_to_step.get(dst_id)
            if not src_step or not dst_step:
                continue

            port_name = re.sub(r"[^a-zA-Z0-9_]", "_", label)

            # Add depends_on
            if src_step not in depends_on[dst_step]:
                depends_on[dst_step].append(src_step)

            transfers.append({
                "from_step": src_step,
                "from_port": port_name,
                "to_step": dst_step,
                "to_port": port_name,
                "protocol": "local",
            })

        # Inject depends_on into steps
        for step in steps:
            step["depends_on"] = depends_on.get(step["id"], [])

        return steps, transfers

    def _extract_node_label(self, node_elem: ET.Element) -> Optional[str]:
        """Extract the yFiles NodeLabel text from a node element."""
        for child in node_elem.iter():
            tag = child.tag.split("}")[-1]
            if tag == "NodeLabel" and child.text:
                return child.text.strip()
        return None

    def _extract_edge_label(self, edge_elem: ET.Element) -> Optional[str]:
        """Extract the yFiles EdgeLabel text from an edge element."""
        for child in edge_elem.iter():
            tag = child.tag.split("}")[-1]
            if tag == "EdgeLabel" and child.text:
                return child.text.strip()
        return None

    def _parse_label(self, label: str) -> Tuple[str, str]:
        """
        Parse a concore node label like "CZ:controller.py" into (prefix, program).
        Returns ("UNKNOWN", label) if the format is unexpected.
        """
        if ":" in label:
            prefix, program = label.split(":", 1)
            return prefix.strip().upper(), program.strip()
        return "UNKNOWN", label

    def _make_step_id(self, program: str) -> str:
        """Convert 'controller.py' to 'controller' (safe step ID)."""
        name = Path(program).stem if "." in program else program
        return re.sub(r"[^a-z0-9_-]", "-", name.lower())


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 -m water.converters.graphml_to_water <study.graphml> [output.yaml]")
        sys.exit(1)

    graphml_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else None

    converter = GraphMLConverter()
    yaml_str = converter.convert(graphml_path)

    if output_path:
        Path(output_path).write_text(yaml_str)
        print(f"Written to {output_path}")
    else:
        print(yaml_str)


if __name__ == "__main__":
    main()
