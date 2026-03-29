"""
Unit tests for WATER workflow schema (Pydantic models).
Run with: pytest tests/test_schema.py -v
"""
import pytest
from pydantic import ValidationError

from water.schema.workflow import (
    ComputeProfile, DataPort, DataTransfer, LatencyProfile,
    PrivacyLevel, StepStatus, TransferProtocol,
    WaterWorkflow, WorkflowIntent, WorkflowStep,
)


def make_step(step_id="step-a", deps=None):
    return WorkflowStep(
        id=step_id,
        name="Test Step",
        image="test/image:latest",
        depends_on=deps or [],
    )


class TestWorkflowStep:
    def test_basic_step_valid(self):
        step = make_step()
        assert step.id == "step-a"
        assert step.retry_count == 2
        assert step.depends_on == []

    def test_step_id_no_spaces(self):
        with pytest.raises(ValidationError, match="must not contain spaces"):
            WorkflowStep(id="step a", name="Bad", image="x:y")

    def test_step_retry_bounds(self):
        with pytest.raises(ValidationError):
            WorkflowStep(id="s", name="x", image="x:y", retry_count=11)
        with pytest.raises(ValidationError):
            WorkflowStep(id="s", name="x", image="x:y", retry_count=-1)

    def test_step_with_ports(self):
        step = WorkflowStep(
            id="png-extraction",
            name="PNG Extractor",
            image="niffler/png-extraction:latest",
            inputs=[DataPort(name="dicom_in", path="dicom/")],
            outputs=[DataPort(name="png_out", path="pngs/")],
        )
        assert len(step.inputs) == 1
        assert step.inputs[0].name == "dicom_in"


class TestWaterWorkflow:
    def _simple_workflow(self):
        return WaterWorkflow(
            name="test-pipeline",
            steps=[
                make_step("cold-extraction"),
                make_step("png-extraction", deps=["cold-extraction"]),
                make_step("meta-extraction", deps=["png-extraction"]),
            ],
        )

    def test_workflow_requires_at_least_one_step(self):
        with pytest.raises(ValidationError):
            WaterWorkflow(name="empty", steps=[])

    def test_workflow_execution_order_linear(self):
        wf = self._simple_workflow()
        waves = wf.execution_order()
        assert waves == [
            ["cold-extraction"],
            ["png-extraction"],
            ["meta-extraction"],
        ]

    def test_workflow_execution_order_parallel(self):
        """Two independent steps should be in the same wave."""
        wf = WaterWorkflow(
            name="parallel",
            steps=[
                make_step("cold-extraction"),
                make_step("png-extraction", deps=["cold-extraction"]),
                make_step("modality-grouping", deps=["cold-extraction"]),
                make_step("merge", deps=["png-extraction", "modality-grouping"]),
            ],
        )
        waves = wf.execution_order()
        assert waves[0] == ["cold-extraction"]
        # png-extraction and modality-grouping can run in parallel
        assert set(waves[1]) == {"png-extraction", "modality-grouping"}
        assert waves[2] == ["merge"]

    def test_get_step(self):
        wf = self._simple_workflow()
        step = wf.get_step("png-extraction")
        assert step is not None
        assert step.image == "test/image:latest"

    def test_get_step_missing_returns_none(self):
        wf = self._simple_workflow()
        assert wf.get_step("does-not-exist") is None

    def test_global_env_propagated(self):
        wf = WaterWorkflow(
            name="env-test",
            steps=[make_step()],
            global_env={"LOG_LEVEL": "INFO"},
        )
        assert wf.global_env["LOG_LEVEL"] == "INFO"


class TestWorkflowIntent:
    def test_defaults(self):
        intent = WorkflowIntent()
        assert intent.privacy == PrivacyLevel.PUBLIC
        assert intent.latency == LatencyProfile.BATCH
        assert intent.compute == ComputeProfile.CPU_LIGHT
        assert intent.tags == {}

    def test_strict_local(self):
        intent = WorkflowIntent(privacy=PrivacyLevel.STRICT_LOCAL)
        assert intent.privacy == PrivacyLevel.STRICT_LOCAL

    def test_tags_stored(self):
        intent = WorkflowIntent(tags={"region": "alaska", "project": "radiology-ai"})
        assert intent.tags["region"] == "alaska"

    def test_workflow_intent_embedded(self):
        wf = WaterWorkflow(
            name="privacy-test",
            intent=WorkflowIntent(
                privacy=PrivacyLevel.STRICT_LOCAL,
                compute=ComputeProfile.GPU_REQUIRED,
            ),
            steps=[WorkflowStep(id="s1", name="s1", image="myorg/img:latest")],
        )
        assert wf.intent.privacy == PrivacyLevel.STRICT_LOCAL
        assert wf.intent.compute == ComputeProfile.GPU_REQUIRED

    def test_workflow_default_intent_is_public(self):
        wf = WaterWorkflow(
            name="default-intent",
            steps=[WorkflowStep(id="s1", name="s1", image="myorg/img:latest")],
        )
        assert wf.intent.privacy == PrivacyLevel.PUBLIC


class TestDataTransfer:
    def test_valid_transfer(self):
        t = DataTransfer(
            from_step="a",
            from_port="out",
            to_step="b",
            to_port="in",
        )
        assert t.protocol == TransferProtocol.SSH

    def test_transfer_local_protocol(self):
        t = DataTransfer(
            from_step="a", from_port="out",
            to_step="b", to_port="in",
            protocol=TransferProtocol.LOCAL,
        )
        assert t.protocol == TransferProtocol.LOCAL
