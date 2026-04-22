"""Glue custom transform for Manhattan DataWays yellow taxi ETL.

This module is written so the core transformation logic can be exercised
locally without AWS Glue libraries, while still exposing an entrypoint that
fits Glue Studio custom-transform usage.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, MutableMapping, Optional, Sequence


CANONICAL_OUTPUT_KEY = "processed"
DEFAULT_INPUT_KEY = "ChangeSchema_node1745291490952"


@dataclass(frozen=True)
class TransformContract:
    """Documents the current Glue node contract for this transform."""

    input_collection_key: str = DEFAULT_INPUT_KEY
    output_collection_key: str = CANONICAL_OUTPUT_KEY
    writes_quarantine_output: bool = False


def get_transform_contract() -> TransformContract:
    """Return the declared I/O contract for docs, tests, and Glue wiring."""

    return TransformContract()


def transform_records(records: Iterable[MutableMapping[str, Any]]) -> List[Dict[str, Any]]:
    """Phase-1 placeholder for row transformation.

    The skeleton intentionally passes records through unchanged so that the
    function is runnable before schema normalization and validation land.
    """

    return [dict(record) for record in records]


def transform_glue_collection(
    frame_collection: Any,
    *,
    input_key: str = DEFAULT_INPUT_KEY,
    output_key: str = CANONICAL_OUTPUT_KEY,
) -> Dict[str, Any]:
    """Phase-1 placeholder for Glue collection handling.

    Phase 1 keeps the adapter intentionally lightweight. Later phases will
    replace this with DynamicFrame-aware logic and observability.
    """

    if frame_collection is None:
        raise ValueError("frame_collection is required")

    return {
        "input_key": input_key,
        "output_key": output_key,
        "frame_collection": frame_collection,
    }


def glue_studio_transform(
    glue_context: Any,
    frame_collection: Any,
    *,
    input_key: str = DEFAULT_INPUT_KEY,
    output_key: str = CANONICAL_OUTPUT_KEY,
) -> Dict[str, Any]:
    """Glue Studio-compatible entrypoint wrapper.

    Glue Studio custom transforms commonly call a top-level function and pass
    the current Glue context plus a DynamicFrameCollection. This wrapper keeps
    the callable stable while the implementation evolves across phases.
    """

    del glue_context
    return transform_glue_collection(
        frame_collection,
        input_key=input_key,
        output_key=output_key,
    )


__all__: Sequence[str] = (
    "CANONICAL_OUTPUT_KEY",
    "DEFAULT_INPUT_KEY",
    "TransformContract",
    "get_transform_contract",
    "glue_studio_transform",
    "transform_glue_collection",
    "transform_records",
)
