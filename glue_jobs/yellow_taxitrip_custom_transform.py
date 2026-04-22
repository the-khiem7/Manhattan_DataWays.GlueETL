"""Glue custom transform for Manhattan DataWays yellow taxi ETL.

This module is written so the core transformation logic can be exercised
locally without AWS Glue libraries, while still exposing an entrypoint that
fits Glue Studio custom-transform usage.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence


CANONICAL_OUTPUT_KEY = "processed"
DEFAULT_INPUT_KEY = "ChangeSchema_node1745291490952"
CANONICAL_COLUMNS: Sequence[str] = (
    "vendorid",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "ratecodeid",
    "store_and_fwd_flag",
    "pulocationid",
    "dolocationid",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
    "cbd_congestion_fee",
    "partition_0",
)
RAW_TO_CANONICAL_MAP: Mapping[str, str] = {
    "VendorID": "vendorid",
    "vendorid": "vendorid",
    "tpep_pickup_datetime": "tpep_pickup_datetime",
    "tpep_dropoff_datetime": "tpep_dropoff_datetime",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "RatecodeID": "ratecodeid",
    "ratecodeid": "ratecodeid",
    "store_and_fwd_flag": "store_and_fwd_flag",
    "PULocationID": "pulocationid",
    "pulocationid": "pulocationid",
    "DOLocationID": "dolocationid",
    "dolocationid": "dolocationid",
    "payment_type": "payment_type",
    "fare_amount": "fare_amount",
    "extra": "extra",
    "mta_tax": "mta_tax",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "total_amount": "total_amount",
    "congestion_surcharge": "congestion_surcharge",
    "Airport_fee": "airport_fee",
    "airport_fee": "airport_fee",
    "cbd_congestion_fee": "cbd_congestion_fee",
    "partition_0": "partition_0",
}
INTEGER_COLUMNS = frozenset(
    {
        "vendorid",
        "ratecodeid",
        "pulocationid",
        "dolocationid",
        "payment_type",
    }
)
FLOAT_COLUMNS = frozenset(
    {
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "airport_fee",
        "cbd_congestion_fee",
    }
)
TIMESTAMP_COLUMNS = frozenset(
    {
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
    }
)


@dataclass(frozen=True)
class TransformContract:
    """Documents the current Glue node contract for this transform."""

    input_collection_key: str = DEFAULT_INPUT_KEY
    output_collection_key: str = CANONICAL_OUTPUT_KEY
    writes_quarantine_output: bool = False


def get_transform_contract() -> TransformContract:
    """Return the declared I/O contract for docs, tests, and Glue wiring."""

    return TransformContract()


def _coerce_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, Decimal):
        return int(value)
    if isinstance(value, float):
        return int(value)
    return int(str(value).strip())


def _coerce_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float, Decimal)):
        return float(value)
    return float(str(value).strip())


def _coerce_timestamp(value: Any) -> Optional[datetime]:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value
    normalized_value = str(value).strip().replace("T", " ")
    return datetime.fromisoformat(normalized_value)


def _normalize_store_and_fwd_flag(value: Any) -> Optional[str]:
    if value is None or value == "":
        return None
    normalized_value = str(value).strip().upper()
    return normalized_value


def normalize_record(record: Mapping[str, Any]) -> Dict[str, Any]:
    """Map raw Glue/parquet field names into the canonical schema."""

    normalized: Dict[str, Any] = {column: None for column in CANONICAL_COLUMNS}

    for source_key, value in record.items():
        canonical_key = RAW_TO_CANONICAL_MAP.get(source_key, source_key.lower())
        normalized[canonical_key] = value

    for column in INTEGER_COLUMNS:
        normalized[column] = _coerce_int(normalized.get(column))

    for column in FLOAT_COLUMNS:
        normalized[column] = _coerce_float(normalized.get(column))

    for column in TIMESTAMP_COLUMNS:
        normalized[column] = _coerce_timestamp(normalized.get(column))

    normalized["store_and_fwd_flag"] = _normalize_store_and_fwd_flag(
        normalized.get("store_and_fwd_flag")
    )

    if normalized.get("partition_0") is not None:
        normalized["partition_0"] = str(normalized["partition_0"]).strip()

    return normalized


def transform_records(records: Iterable[MutableMapping[str, Any]]) -> List[Dict[str, Any]]:
    """Normalize schema and basic types into the canonical row shape."""

    return [normalize_record(record) for record in records]


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
    "CANONICAL_COLUMNS",
    "DEFAULT_INPUT_KEY",
    "RAW_TO_CANONICAL_MAP",
    "TransformContract",
    "get_transform_contract",
    "glue_studio_transform",
    "normalize_record",
    "transform_glue_collection",
    "transform_records",
)
