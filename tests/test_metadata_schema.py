"""Tests for engine.metadata_schema: Pydantic models and structural validation."""
import json

import pytest
from pydantic import ValidationError

from engine.metadata_schema import MetadataError, MetadataFile, load_metadata
from tests.fixtures import clone_valid_metadata


def test_valid_metadata_parses_into_expected_node_counts():
    metadata = MetadataFile.model_validate(clone_valid_metadata())

    assert len(metadata.dataflows) == 1
    dataflow = metadata.dataflows[0]
    assert dataflow.name == "prueba-acceso"
    assert len(dataflow.inputs) == 2
    assert len(dataflow.transformations) == 3
    assert len(dataflow.outputs) == 4


def test_input_waits_defaults_to_empty_list_when_absent():
    metadata = MetadataFile.model_validate(clone_valid_metadata())
    dataflow = metadata.dataflows[0]

    demo_data = next(n for n in dataflow.inputs if n.name == "demo_data")
    parquet_data = next(n for n in dataflow.inputs if n.name == "parquet_data")

    assert demo_data.waits == []
    assert parquet_data.waits == ["write_last_file"]


def test_transformations_may_be_an_empty_list():
    data = clone_valid_metadata()
    data["dataflows"][0]["transformations"] = []
    # drop the outputs that depend on transformation nodes so referential
    # integrity still holds with no transformations at all
    data["dataflows"][0]["outputs"] = [
        {
            "name": "write_last_file",
            "type": "file",
            "input": "demo_data",
            "config": {"path": "/out", "format": "parquet", "save_mode": "overwrite"},
        }
    ]

    metadata = MetadataFile.model_validate(data)

    assert metadata.dataflows[0].transformations == []


def test_empty_inputs_list_is_rejected():
    data = clone_valid_metadata()
    data["dataflows"][0]["inputs"] = []

    with pytest.raises(ValidationError):
        MetadataFile.model_validate(data)


def test_empty_outputs_list_is_rejected():
    data = clone_valid_metadata()
    data["dataflows"][0]["outputs"] = []

    with pytest.raises(ValidationError):
        MetadataFile.model_validate(data)


def test_duplicate_name_across_sections_is_rejected():
    data = clone_valid_metadata()
    # reuse an input's name for one of the outputs
    data["dataflows"][0]["outputs"][0]["name"] = "demo_data"

    with pytest.raises(ValidationError, match="duplicate node name"):
        MetadataFile.model_validate(data)


def test_dangling_transformation_input_is_rejected():
    data = clone_valid_metadata()
    data["dataflows"][0]["transformations"][1]["input"] = "does_not_exist"  # new_fields.input

    with pytest.raises(ValidationError, match="does not match any existing node name"):
        MetadataFile.model_validate(data)


def test_dangling_output_input_is_rejected():
    data = clone_valid_metadata()
    data["dataflows"][0]["outputs"][0]["input"] = "does_not_exist"

    with pytest.raises(ValidationError, match="does not match any existing node name"):
        MetadataFile.model_validate(data)


def test_dangling_waits_reference_is_rejected():
    data = clone_valid_metadata()
    data["dataflows"][0]["inputs"][1]["waits"] = ["nonexistent_node"]

    with pytest.raises(ValidationError, match="waits"):
        MetadataFile.model_validate(data)


def test_table_output_merge_without_primary_key_is_rejected():
    data = clone_valid_metadata()
    del data["dataflows"][0]["outputs"][2]["config"]["primary_key"]  # write_delta_merge

    with pytest.raises(ValidationError, match="primary_key"):
        MetadataFile.model_validate(data)


def test_table_output_append_with_primary_key_is_rejected():
    data = clone_valid_metadata()
    data["dataflows"][0]["outputs"][3]["config"]["primary_key"] = ["provincia"]  # write_delta_raw

    with pytest.raises(ValidationError, match="primary_key"):
        MetadataFile.model_validate(data)


def test_unknown_output_type_is_rejected():
    data = clone_valid_metadata()
    data["dataflows"][0]["outputs"][0]["type"] = "database"

    with pytest.raises(ValidationError):
        MetadataFile.model_validate(data)


def test_node_missing_required_field_is_rejected():
    data = clone_valid_metadata()
    del data["dataflows"][0]["inputs"][0]["config"]

    with pytest.raises(ValidationError):
        MetadataFile.model_validate(data)


def test_load_metadata_from_file(tmp_path):
    metadata_path = tmp_path / "metadata.json"
    metadata_path.write_text(json.dumps(clone_valid_metadata()), encoding="utf-8")

    metadata = load_metadata(metadata_path)

    assert metadata.dataflows[0].name == "prueba-acceso"


def test_load_metadata_missing_file_raises_metadata_error(tmp_path):
    with pytest.raises(MetadataError, match="cannot read"):
        load_metadata(tmp_path / "missing.json")


def test_load_metadata_invalid_json_raises_metadata_error(tmp_path):
    metadata_path = tmp_path / "metadata.json"
    metadata_path.write_text("{not valid json", encoding="utf-8")

    with pytest.raises(MetadataError, match="not valid JSON"):
        load_metadata(metadata_path)


def test_load_metadata_schema_violation_raises_metadata_error(tmp_path):
    data = clone_valid_metadata()
    data["dataflows"][0]["inputs"] = []
    metadata_path = tmp_path / "metadata.json"
    metadata_path.write_text(json.dumps(data), encoding="utf-8")

    with pytest.raises(MetadataError, match="failed schema validation"):
        load_metadata(metadata_path)
