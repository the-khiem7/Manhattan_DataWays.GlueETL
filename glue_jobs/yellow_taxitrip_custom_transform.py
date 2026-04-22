"""Glue custom transform for Manhattan DataWays yellow taxi ETL.

This module is written so the core transformation logic can be exercised
locally without AWS Glue libraries, while still exposing an entrypoint that
fits Glue Studio custom-transform usage.
"""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence

try:
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection
except ImportError:  # pragma: no cover - unavailable in local development.
    DynamicFrame = None
    DynamicFrameCollection = None

try:
    from pyspark.sql import DataFrame
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
except ImportError:  # pragma: no cover - unavailable in local development.
    DataFrame = None
    F = None
    Window = None

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
PROCESSED_OUTPUT_COLUMNS: Sequence[str] = (
    *CANONICAL_COLUMNS,
    "trip_duration_hours",
    "warning_reason",
    "is_outlier",
)

LOGGER = logging.getLogger(__name__)
if not LOGGER.handlers:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")


def get_transform_contract() -> TransformContract:
    """Return the declared I/O contract for docs, tests, and Glue wiring."""

    return TransformContract()


def _get_source_candidates(canonical_key: str) -> List[str]:
    candidates = [canonical_key]
    for source_key, mapped_key in RAW_TO_CANONICAL_MAP.items():
        if mapped_key == canonical_key and source_key not in candidates:
            candidates.append(source_key)
    return candidates


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


def summarize_transformed_records(records: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    summary: Dict[str, Any] = {
        "total_records": len(records),
        "valid_records": 0,
        "invalid_records": 0,
        "outlier_records": 0,
        "error_counts": {},
        "warning_counts": {},
    }

    for record in records:
        if record.get("is_valid"):
            summary["valid_records"] += 1
        else:
            summary["invalid_records"] += 1

        if record.get("is_outlier"):
            summary["outlier_records"] += 1

        for reason in filter(None, str(record.get("error_reason", "")).split("|")):
            summary["error_counts"][reason] = summary["error_counts"].get(reason, 0) + 1

        for reason in filter(None, str(record.get("warning_reason", "")).split("|")):
            summary["warning_counts"][reason] = summary["warning_counts"].get(reason, 0) + 1

    return summary


def filter_processed_records(records: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    processed_records: List[Dict[str, Any]] = []
    for record in records:
        if not record.get("is_valid"):
            continue
        processed_records.append(
            {column: record.get(column) for column in PROCESSED_OUTPUT_COLUMNS}
        )
    return processed_records


def _log_summary(summary: Mapping[str, Any]) -> None:
    LOGGER.info(
        "Processed Glue ETL records: total=%s valid=%s invalid=%s outliers=%s",
        summary.get("total_records"),
        summary.get("valid_records"),
        summary.get("invalid_records"),
        summary.get("outlier_records"),
    )
    for reason, count in sorted(summary.get("error_counts", {}).items()):
        LOGGER.info("Rejected rows for %s=%s", reason, count)
    for reason, count in sorted(summary.get("warning_counts", {}).items()):
        LOGGER.info("Warning rows for %s=%s", reason, count)


def run_local_transform(records: Iterable[MutableMapping[str, Any]]) -> Dict[str, Any]:
    transformed_records = transform_records(records)
    summary = summarize_transformed_records(transformed_records)
    processed_records = filter_processed_records(transformed_records)
    _log_summary(summary)
    return {
        "output_key": CANONICAL_OUTPUT_KEY,
        "processed_records": processed_records,
        "summary": summary,
        "transformed_records": transformed_records,
    }


def _load_parquet_records(parquet_path: str) -> List[Dict[str, Any]]:
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - optional local dependency.
        raise RuntimeError(
            "pyarrow is required for local parquet smoke tests"
        ) from exc

    return pq.read_table(parquet_path).to_pylist()


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run the Manhattan DataWays Glue ETL custom transform locally."
    )
    parser.add_argument(
        "parquet_path",
        help="Path to a parquet file to validate locally.",
    )
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Print only the summary payload.",
    )
    args = parser.parse_args(argv)

    result = run_local_transform(_load_parquet_records(args.parquet_path))
    payload: Dict[str, Any] = {"summary": result["summary"]}
    if not args.summary_only:
        payload["processed_sample"] = result["processed_records"][:3]
        payload["invalid_sample"] = [
            record
            for record in result["transformed_records"]
            if not record.get("is_valid")
        ][:3]

    print(json.dumps(payload, default=str, indent=2))
    return 0


def _normalize_spark_input_df(input_df: Any) -> Any:
    if F is None:
        raise RuntimeError("pyspark is required for Glue DataFrame execution")

    available_columns = set(input_df.columns)
    projected_columns = []
    for canonical_key in CANONICAL_COLUMNS:
        source_column = next(
            (candidate for candidate in _get_source_candidates(canonical_key) if candidate in available_columns),
            None,
        )
        if source_column is None:
            projected_column = F.lit(None).alias(canonical_key)
        else:
            projected_column = F.col(source_column).alias(canonical_key)
        projected_columns.append(projected_column)

    normalized_df = input_df.select(*projected_columns)

    for column in INTEGER_COLUMNS:
        normalized_df = normalized_df.withColumn(column, F.col(column).cast("int"))

    for column in FLOAT_COLUMNS:
        normalized_df = normalized_df.withColumn(column, F.col(column).cast("double"))

    for column in TIMESTAMP_COLUMNS:
        normalized_df = normalized_df.withColumn(column, F.col(column).cast("timestamp"))

    normalized_df = normalized_df.withColumn(
        "store_and_fwd_flag",
        F.when(
            F.col("store_and_fwd_flag").isNull(),
            F.lit(None),
        ).otherwise(F.upper(F.trim(F.col("store_and_fwd_flag")))),
    )
    normalized_df = normalized_df.withColumn(
        "partition_0",
        F.when(F.col("partition_0").isNull(), F.lit(None)).otherwise(F.trim(F.col("partition_0"))),
    )
    return normalized_df


def _transform_spark_df(input_df: Any) -> Any:
    if F is None or Window is None:
        raise RuntimeError("pyspark is required for Glue DataFrame execution")

    thresholds = ValidationThresholds()
    normalized_df = _normalize_spark_input_df(input_df)
    duplicate_window = Window.partitionBy(
        "vendorid",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "total_amount",
    ).orderBy(F.col("tpep_pickup_datetime"))

    enriched_df = normalized_df.withColumn(
        "trip_duration_hours",
        (
            F.col("tpep_dropoff_datetime").cast("long")
            - F.col("tpep_pickup_datetime").cast("long")
        )
        / F.lit(3600.0),
    ).withColumn(
        "_duplicate_rank",
        F.row_number().over(duplicate_window),
    )

    error_reason_candidates = [
        F.when(F.col(column).isNull(), F.lit(f"missing_{column}"))
        for column in REQUIRED_COLUMNS
    ] + [
        F.when(
            F.col("store_and_fwd_flag").isNotNull()
            & (~F.col("store_and_fwd_flag").isin(*sorted(VALID_STORE_AND_FWD_FLAGS))),
            F.lit("invalid_store_and_fwd_flag"),
        ),
        F.when(
            F.col("payment_type").isNotNull()
            & (~F.col("payment_type").isin(*sorted(VALID_PAYMENT_TYPES))),
            F.lit("invalid_payment_type"),
        ),
        F.when(F.col("_duplicate_rank") > 1, F.lit("duplicate_record")),
        F.when(F.col("trip_duration_hours") < 0, F.lit("dropoff_before_pickup")),
        F.when(
            F.col("trip_duration_hours") > F.lit(thresholds.max_trip_duration_hours),
            F.lit("trip_duration_exceeds_max"),
        ),
        F.when(F.col("passenger_count") <= 0, F.lit("passenger_count_le_zero")),
        F.when(
            F.col("passenger_count") > F.lit(thresholds.max_passenger_count),
            F.lit("passenger_count_exceeds_max"),
        ),
        F.when(F.col("trip_distance") <= 0, F.lit("trip_distance_le_zero")),
        F.when(
            F.col("trip_distance") > F.lit(thresholds.max_trip_distance),
            F.lit("trip_distance_exceeds_max"),
        ),
        F.when(F.col("fare_amount") < 0, F.lit("fare_amount_lt_zero")),
        F.when(
            F.col("total_amount") < F.col("fare_amount"),
            F.lit("total_amount_lt_fare_amount"),
        ),
        F.when(
            (F.col("payment_type") == F.lit(2)) & (F.coalesce(F.col("tip_amount"), F.lit(0.0)) > 0),
            F.lit("cash_payment_with_tip"),
        ),
    ]
    warning_reason_candidates = [
        F.when(
            F.col("trip_duration_hours") > F.lit(thresholds.warning_trip_duration_hours),
            F.lit("trip_duration_outlier"),
        ),
        F.when(
            F.col("trip_distance") > F.lit(thresholds.warning_trip_distance),
            F.lit("trip_distance_outlier"),
        ),
        F.when(
            F.col("fare_amount") > F.lit(thresholds.warning_fare_amount),
            F.lit("fare_amount_outlier"),
        ),
    ]

    enriched_df = enriched_df.withColumn(
        "_error_reasons",
        F.array_remove(F.array(*error_reason_candidates), F.lit(None)),
    ).withColumn(
        "_warning_reasons",
        F.array_remove(F.array(*warning_reason_candidates), F.lit(None)),
    )

    enriched_df = enriched_df.withColumn(
        "error_reason",
        F.concat_ws("|", F.col("_error_reasons")),
    ).withColumn(
        "warning_reason",
        F.concat_ws("|", F.col("_warning_reasons")),
    ).withColumn(
        "is_valid",
        F.size(F.col("_error_reasons")) == 0,
    ).withColumn(
        "is_outlier",
        F.size(F.col("_warning_reasons")) > 0,
    )

    return enriched_df.drop("_duplicate_rank", "_error_reasons", "_warning_reasons")


def _summarize_spark_df(enriched_df: Any) -> Dict[str, Any]:
    if F is None:
        raise RuntimeError("pyspark is required for Glue DataFrame execution")

    aggregate_row = enriched_df.agg(
        F.count("*").alias("total_records"),
        F.sum(F.when(F.col("is_valid"), F.lit(1)).otherwise(F.lit(0))).alias("valid_records"),
        F.sum(F.when(~F.col("is_valid"), F.lit(1)).otherwise(F.lit(0))).alias("invalid_records"),
        F.sum(F.when(F.col("is_outlier"), F.lit(1)).otherwise(F.lit(0))).alias("outlier_records"),
    ).collect()[0]

    summary: Dict[str, Any] = {
        "total_records": int(aggregate_row["total_records"]),
        "valid_records": int(aggregate_row["valid_records"]),
        "invalid_records": int(aggregate_row["invalid_records"]),
        "outlier_records": int(aggregate_row["outlier_records"]),
        "error_counts": {},
        "warning_counts": {},
    }

    error_rows = (
        enriched_df.where(F.col("error_reason") != "")
        .select(F.explode(F.split(F.col("error_reason"), r"\|")).alias("reason"))
        .groupBy("reason")
        .count()
        .collect()
    )
    for row in error_rows:
        summary["error_counts"][row["reason"]] = int(row["count"])

    warning_rows = (
        enriched_df.where(F.col("warning_reason") != "")
        .select(F.explode(F.split(F.col("warning_reason"), r"\|")).alias("reason"))
        .groupBy("reason")
        .count()
        .collect()
    )
    for row in warning_rows:
        summary["warning_counts"][row["reason"]] = int(row["count"])

    return summary


def transform_glue_collection(
    glue_context: Any,
    frame_collection: Any,
    *,
    input_key: str = DEFAULT_INPUT_KEY,
    output_key: str = CANONICAL_OUTPUT_KEY,
) -> Dict[str, Any]:
    """Run the transform in Glue if possible, otherwise use a local fallback."""

    if frame_collection is None:
        raise ValueError("frame_collection is required")

    if (
        DynamicFrameCollection is not None
        and DynamicFrame is not None
        and hasattr(frame_collection, "select")
        and glue_context is not None
    ):
        input_frame = frame_collection.select(input_key)
        input_df = input_frame.toDF()
        enriched_df = _transform_spark_df(input_df)
        summary = _summarize_spark_df(enriched_df)
        _log_summary(summary)
        processed_df = enriched_df.where(F.col("is_valid")).select(*PROCESSED_OUTPUT_COLUMNS)
        processed_dynamic_frame = DynamicFrame.fromDF(processed_df, glue_context, output_key)
        return DynamicFrameCollection({output_key: processed_dynamic_frame}, glue_context)

    if isinstance(frame_collection, Mapping):
        input_records = frame_collection.get(input_key, frame_collection.get(output_key))
        if input_records is None:
            raise KeyError(f"Could not find input records for key '{input_key}'")
    else:
        input_records = frame_collection

    local_result = run_local_transform(input_records)
    return {
        "output_key": output_key,
        "processed_records": local_result["processed_records"],
        "summary": local_result["summary"],
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

    return transform_glue_collection(
        glue_context,
        frame_collection,
        input_key=input_key,
        output_key=output_key,
    )


__all__: Sequence[str] = (
    "CANONICAL_OUTPUT_KEY",
    "CANONICAL_COLUMNS",
    "DEFAULT_INPUT_KEY",
    "PROCESSED_OUTPUT_COLUMNS",
    "RAW_TO_CANONICAL_MAP",
    "TransformContract",
    "ValidationThresholds",
    "filter_processed_records",
    "get_transform_contract",
    "glue_studio_transform",
    "main",
    "normalize_record",
    "run_local_transform",
    "summarize_transformed_records",
    "transform_glue_collection",
    "transform_records",
)


if __name__ == "__main__":
    raise SystemExit(main())
