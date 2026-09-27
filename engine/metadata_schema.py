"""
Pydantic models for ``metadata.json``.

``metadata.json`` describes one or more *dataflows*. Each dataflow is a DAG
of nodes split into three sections — ``inputs``, ``transformations`` and
``outputs`` — that all share a single ``name`` namespace: a name declared in
``inputs`` cannot be reused in ``transformations`` or ``outputs``, and vice
versa.

Node shapes are discriminated by their ``type`` field:
  - inputs:          type="file"
  - transformations: type="filter" | "add_fields" | "group"
  - outputs:         type="file" | "table"

Each of those types has its own ``config`` shape, modelled below.

This module only parses and validates the file. It does not build the DAG
structure (see :mod:`engine.graph`) and does not touch Spark.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class MetadataError(Exception):
    """Raised when metadata.json cannot be read, parsed as JSON, or validated against the schema."""


def _non_blank(value: str, *, field_name: str) -> str:
    if not value.strip():
        raise ValueError(f"'{field_name}' must not be blank")
    return value


# --------------------------------------------------------------------------
# inputs (type=file)
# --------------------------------------------------------------------------


class InputFileConfig(BaseModel):
    """``config`` shape for an input node of type=file."""

    model_config = ConfigDict(extra="forbid")

    path: str
    format: str

    @field_validator("path")
    @classmethod
    def _validate_path(cls, v: str) -> str:
        return _non_blank(v, field_name="config.path")

    @field_validator("format")
    @classmethod
    def _validate_format(cls, v: str) -> str:
        return _non_blank(v, field_name="config.format")


class FileInputNode(BaseModel):
    """
    An ``inputs`` node (currently the only supported input type is "file").

    ``waits`` is an optional list of node names whose execution must be
    forced to completion before this input is resolved (read). It is an
    order-only dependency, not a data dependency: see :mod:`engine.graph`
    for how it is represented in the DAG structure.
    """

    model_config = ConfigDict(extra="forbid")

    name: str
    type: Literal["file"]
    config: InputFileConfig
    options: dict[str, str] | None = None
    waits: list[str] = Field(default_factory=list)

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str) -> str:
        return _non_blank(v, field_name="name")


# --------------------------------------------------------------------------
# transformations (type=filter | add_fields | group)
# --------------------------------------------------------------------------


class FilterConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    filter: str

    @field_validator("filter")
    @classmethod
    def _validate_filter(cls, v: str) -> str:
        return _non_blank(v, field_name="config.filter")


class FilterTransformation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    type: Literal["filter"]
    input: str
    config: FilterConfig

    @field_validator("name", "input")
    @classmethod
    def _validate_non_blank_fields(cls, v: str, info) -> str:
        return _non_blank(v, field_name=info.field_name)


class AddFieldSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    expression: str

    @field_validator("name", "expression")
    @classmethod
    def _validate_non_blank_fields(cls, v: str, info) -> str:
        return _non_blank(v, field_name=f"config.fields[].{info.field_name}")


class AddFieldsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    fields: list[AddFieldSpec] = Field(min_length=1)


class AddFieldsTransformation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    type: Literal["add_fields"]
    input: str
    config: AddFieldsConfig

    @field_validator("name", "input")
    @classmethod
    def _validate_non_blank_fields(cls, v: str, info) -> str:
        return _non_blank(v, field_name=info.field_name)


class GroupConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    group_fields: list[str] = Field(min_length=1)
    aggregations: list[str] = Field(min_length=1)


class GroupTransformation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    type: Literal["group"]
    input: str
    config: GroupConfig

    @field_validator("name", "input")
    @classmethod
    def _validate_non_blank_fields(cls, v: str, info) -> str:
        return _non_blank(v, field_name=info.field_name)


TransformationNode = Annotated[
    Union[FilterTransformation, AddFieldsTransformation, GroupTransformation],
    Field(discriminator="type"),
]


# --------------------------------------------------------------------------
# outputs (type=file | table)
# --------------------------------------------------------------------------


class FileOutputConfig(BaseModel):
    """``config`` shape for an output node of type=file."""

    model_config = ConfigDict(extra="forbid")

    path: str
    format: str
    save_mode: Literal["overwrite", "append"]
    partition: str | None = None

    @field_validator("path", "format")
    @classmethod
    def _validate_non_blank_fields(cls, v: str, info) -> str:
        return _non_blank(v, field_name=f"config.{info.field_name}")


class TableOutputConfig(BaseModel):
    """
    ``config`` shape for an output node of type=table.

    ``primary_key`` is required (and must be non-empty) when
    ``save_mode="merge"``, and must be omitted when ``save_mode="append"``.
    """

    model_config = ConfigDict(extra="forbid")

    table: str
    save_mode: Literal["merge", "append"]
    primary_key: list[str] | None = None

    @field_validator("table")
    @classmethod
    def _validate_table(cls, v: str) -> str:
        return _non_blank(v, field_name="config.table")

    @model_validator(mode="after")
    def _validate_primary_key_matches_save_mode(self) -> "TableOutputConfig":
        if self.save_mode == "merge" and not self.primary_key:
            raise ValueError(
                "table output config with save_mode='merge' requires a non-empty 'primary_key'"
            )
        if self.save_mode == "append" and self.primary_key is not None:
            raise ValueError(
                "table output config with save_mode='append' must not declare 'primary_key'"
            )
        return self


class FileOutputNode(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    type: Literal["file"]
    input: str
    config: FileOutputConfig

    @field_validator("name", "input")
    @classmethod
    def _validate_non_blank_fields(cls, v: str, info) -> str:
        return _non_blank(v, field_name=info.field_name)


class TableOutputNode(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    type: Literal["table"]
    input: str
    config: TableOutputConfig

    @field_validator("name", "input")
    @classmethod
    def _validate_non_blank_fields(cls, v: str, info) -> str:
        return _non_blank(v, field_name=info.field_name)


OutputNode = Annotated[Union[FileOutputNode, TableOutputNode], Field(discriminator="type")]


# --------------------------------------------------------------------------
# dataflow / top-level file
# --------------------------------------------------------------------------


class Dataflow(BaseModel):
    """
    One dataflow: a DAG of `inputs` / `transformations` / `outputs` nodes
    sharing a single `name` namespace.
    """

    model_config = ConfigDict(extra="forbid")

    name: str
    inputs: list[FileInputNode] = Field(min_length=1)
    transformations: list[TransformationNode] = Field(default_factory=list)
    outputs: list[OutputNode] = Field(min_length=1)

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str) -> str:
        return _non_blank(v, field_name="name")

    @model_validator(mode="after")
    def _validate_structural_integrity(self) -> "Dataflow":
        all_names = (
            [n.name for n in self.inputs]
            + [n.name for n in self.transformations]
            + [n.name for n in self.outputs]
        )

        seen: set[str] = set()
        duplicates: set[str] = set()
        for n in all_names:
            if n in seen:
                duplicates.add(n)
            seen.add(n)
        if duplicates:
            raise ValueError(
                f"dataflow '{self.name}': duplicate node name(s) {sorted(duplicates)} — "
                "'name' must be unique across inputs, transformations and outputs"
            )

        name_set = set(all_names)

        for node in (*self.transformations, *self.outputs):
            if node.input not in name_set:
                raise ValueError(
                    f"dataflow '{self.name}': node '{node.name}' has input='{node.input}' "
                    "which does not match any existing node name"
                )

        for input_node in self.inputs:
            for waited_name in input_node.waits:
                if waited_name not in name_set:
                    raise ValueError(
                        f"dataflow '{self.name}': input node '{input_node.name}' declares "
                        f"waits=[...'{waited_name}'...] which does not match any existing node name"
                    )

        return self


class MetadataFile(BaseModel):
    """Top-level ``metadata.json`` document."""

    model_config = ConfigDict(extra="forbid")

    dataflows: list[Dataflow] = Field(min_length=1)


def load_metadata(path: str | Path) -> MetadataFile:
    """
    Read, parse and validate a ``metadata.json`` file.

    Raises
    ------
    MetadataError
        If the file cannot be read, is not valid JSON, or fails schema
        validation (missing/empty inputs or outputs, duplicate names,
        dangling `input`/`waits` references, wrong `config` shape for a
        node's `type`, etc). The original cause is chained via `__cause__`.
    """
    path = Path(path)

    try:
        raw_text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise MetadataError(f"cannot read metadata file '{path}': {exc}") from exc

    try:
        raw_data = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        raise MetadataError(f"metadata file '{path}' is not valid JSON: {exc}") from exc

    try:
        return MetadataFile.model_validate(raw_data)
    except Exception as exc:  # pydantic.ValidationError, but keep this module pydantic-import-light
        raise MetadataError(f"metadata file '{path}' failed schema validation:\n{exc}") from exc
