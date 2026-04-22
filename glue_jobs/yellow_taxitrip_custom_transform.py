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


@dataclass(frozen=True)
class ValidationThresholds:
    max_trip_duration_hours: float = 24.0
    max_passenger_count: int = 6
    max_trip_distance: float = 100.0
    warning_trip_duration_hours: float = 4.0
    warning_trip_distance: float = 50.0
    warning_fare_amount: float = 300.0


VALID_STORE_AND_FWD_FLAGS = frozenset({"Y", "N"})
VALID_PAYMENT_TYPES = frozenset({1, 2, 3, 4, 5, 6})
REQUIRED_COLUMNS = frozenset(
    {
        "vendorid",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "total_amount",
    }
)
AUDIT_COLUMNS: Sequence[str] = (
    "trip_duration_hours",
    "is_valid",
    "error_reason",
    "warning_reason",
    "is_outlier",
)


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


def _append_reason(reasons: List[str], reason: str) -> None:
    if reason not in reasons:
        reasons.append(reason)


def _build_duplicate_key(record: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        record.get("vendorid"),
        record.get("tpep_pickup_datetime"),
        record.get("tpep_dropoff_datetime"),
        record.get("passenger_count"),
        record.get("trip_distance"),
        record.get("fare_amount"),
        record.get("total_amount"),
    )


def _derive_trip_duration_hours(record: Mapping[str, Any]) -> Optional[float]:
    pickup_time = record.get("tpep_pickup_datetime")
    dropoff_time = record.get("tpep_dropoff_datetime")
    if pickup_time is None or dropoff_time is None:
        return None
    return (dropoff_time - pickup_time).total_seconds() / 3600.0


def _validate_record(
    record: Dict[str, Any],
    *,
    duplicate_seen: set[tuple[Any, ...]],
    thresholds: ValidationThresholds,
) -> Dict[str, Any]:
    enriched = dict(record)
    error_reasons: List[str] = []
    warning_reasons: List[str] = []

    for column in REQUIRED_COLUMNS:
        if enriched.get(column) is None:
            _append_reason(error_reasons, f"missing_{column}")

    trip_duration_hours = _derive_trip_duration_hours(enriched)
    enriched["trip_duration_hours"] = trip_duration_hours

    duplicate_key = _build_duplicate_key(enriched)
    if duplicate_key in duplicate_seen:
        _append_reason(error_reasons, "duplicate_record")
    else:
        duplicate_seen.add(duplicate_key)

    store_and_fwd_flag = enriched.get("store_and_fwd_flag")
    if store_and_fwd_flag is not None and store_and_fwd_flag not in VALID_STORE_AND_FWD_FLAGS:
        _append_reason(error_reasons, "invalid_store_and_fwd_flag")

    payment_type = enriched.get("payment_type")
    if payment_type is not None and payment_type not in VALID_PAYMENT_TYPES:
        _append_reason(error_reasons, "invalid_payment_type")

    if trip_duration_hours is not None:
        if trip_duration_hours < 0:
            _append_reason(error_reasons, "dropoff_before_pickup")
        if trip_duration_hours > thresholds.max_trip_duration_hours:
            _append_reason(error_reasons, "trip_duration_exceeds_max")
        elif trip_duration_hours > thresholds.warning_trip_duration_hours:
            _append_reason(warning_reasons, "trip_duration_outlier")

    passenger_count = enriched.get("passenger_count")
    if passenger_count is not None:
        if passenger_count <= 0:
            _append_reason(error_reasons, "passenger_count_le_zero")
        if passenger_count > thresholds.max_passenger_count:
            _append_reason(error_reasons, "passenger_count_exceeds_max")

    trip_distance = enriched.get("trip_distance")
    if trip_distance is not None:
        if trip_distance <= 0:
            _append_reason(error_reasons, "trip_distance_le_zero")
        if trip_distance > thresholds.max_trip_distance:
            _append_reason(error_reasons, "trip_distance_exceeds_max")
        elif trip_distance > thresholds.warning_trip_distance:
            _append_reason(warning_reasons, "trip_distance_outlier")

    fare_amount = enriched.get("fare_amount")
    if fare_amount is not None:
        if fare_amount < 0:
            _append_reason(error_reasons, "fare_amount_lt_zero")
        elif fare_amount > thresholds.warning_fare_amount:
            _append_reason(warning_reasons, "fare_amount_outlier")

    total_amount = enriched.get("total_amount")
    if total_amount is not None and fare_amount is not None and total_amount < fare_amount:
        _append_reason(error_reasons, "total_amount_lt_fare_amount")

    tip_amount = enriched.get("tip_amount") or 0.0
    if payment_type == 2 and tip_amount > 0:
        _append_reason(error_reasons, "cash_payment_with_tip")

    enriched["error_reason"] = "|".join(error_reasons)
    enriched["warning_reason"] = "|".join(warning_reasons)
    enriched["is_valid"] = not error_reasons
    enriched["is_outlier"] = bool(warning_reasons)
    return enriched


def transform_records(records: Iterable[MutableMapping[str, Any]]) -> List[Dict[str, Any]]:
    """Normalize, enrich, and validate rows in the canonical schema."""

    thresholds = ValidationThresholds()
    duplicate_seen: set[tuple[Any, ...]] = set()
    transformed_records: List[Dict[str, Any]] = []
    for record in records:
        normalized_record = normalize_record(record)
        transformed_records.append(
            _validate_record(
                normalized_record,
                duplicate_seen=duplicate_seen,
                thresholds=thresholds,
            )
        )
    return transformed_records


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
    "ValidationThresholds",
    "get_transform_contract",
    "glue_studio_transform",
    "normalize_record",
    "transform_glue_collection",
    "transform_records",
)
