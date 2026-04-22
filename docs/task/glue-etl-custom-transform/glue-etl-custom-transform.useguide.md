# Glue ETL Custom Transform Use Guide

## Purpose

This guide describes how the planned Glue custom transform should be used once implemented for the Manhattan DataWays `Glue ETL Job`.

It assumes the pipeline position shown in [Architecture.png](/d:/SourceCode/FirstCloudJourney/Manhattan_DataWays.GlueETL/Architecture.png):

`AWS Glue Crawler -> AWS Glue Catalog -> Glue ETL Job`

## Intended Job Shape

The job should be configured around this simplified flow:

`AWS Glue Data Catalog -> Change Schema -> Custom Transform -> Processed S3 target`

If Glue Studio still requires collection-style wiring for the custom node, the practical flow can remain:

`AWS Glue Data Catalog -> Change Schema -> Custom Transform -> Select From Collection -> Processed S3 target`

The key requirement is unchanged:

- there is no quarantine sink
- rejected rows are not persisted to a separate S3 prefix

## Intended Script Location

Expected script path:

- `glue_jobs/yellow_taxitrip_custom_transform.py`

## Expected Transform Responsibilities

The custom transform should:

- accept the curated Glue input frame after schema cleanup
- normalize column names or values if they are not already normalized
- derive helper fields such as trip duration
- validate each row against business rules
- keep only valid rows for downstream output
- log rejection counts and rejection reasons

## Canonical Input Expectations

The source data is based on TLC yellow taxi trip parquet data. Before using the transform, confirm that the upstream schema is stable and that the code knows whether it receives:

- raw-style names such as `VendorID`, `RatecodeID`, `PULocationID`
- normalized names such as `vendorid`, `ratecodeid`, `pulocationid`

Do not assume `DataSchema.md` alone is enough. Validate against the actual Glue Catalog table or the parquet sample when changing the job.

The implementation standardizes on lowercase target-style field names internally. The transform is expected to accept either:

- raw TLC-style names from parquet or Glue Catalog inference
- already-renamed canonical names from `Change Schema`

Current canonical examples:

- `vendorid`
- `ratecodeid`
- `pulocationid`
- `dolocationid`
- `airport_fee`

## Recommended Validation Rules

Reject rows when any hard-failure rule is true:

- required field is null
- `passenger_count <= 0`
- `trip_distance <= 0`
- `fare_amount < 0`
- `total_amount < fare_amount`
- `dropoff < pickup`
- unsupported payment type
- `payment_type = cash` with positive tip

Retain rows when only soft anomalies are present, if the team decides to keep warnings instead of dropping them:

- unusually high fare
- unusually long duration
- unusually long distance

If warnings are retained, keep the warning columns in the processed dataset only if they are useful for downstream analytics.

The current implementation also enriches each canonical row with audit fields before final output filtering:

- `trip_duration_hours`
- `is_valid`
- `error_reason`
- `warning_reason`
- `is_outlier`

## Recommended Logging

Because rejected rows are not written to S3, logs become the main operational signal.

At minimum, log:

- total input row count
- total accepted row count
- total rejected row count
- rejection counts by rule
- schema version or canonical column mode

If available in the Glue runtime, also publish counters to CloudWatch-compatible metrics.

## Suggested Development Workflow

1. Start with the 50-row parquet sample to validate rule behavior quickly.
2. Confirm the final canonical column names used in code.
3. Implement the transform with explicit rejection reasons.
4. Wire the visual job to a single processed sink.
5. Run the job and inspect logs for rejected-row statistics.
6. Update the roadmap document with actual implementation status.

## Practical Notes For Glue Studio

- Use `Change Schema` for simple casting and renaming, not for dense business validation logic.
- Keep the Python custom transform focused on row-level and cross-column validation.
- The finalized output key is `processed`.
- If the visual node still outputs a collection, ensure the selected collection key is the valid processed output only.
- Remove any leftover quarantine naming from node labels, comments, and sink paths.

The current code keeps these columns in the processed output:

- canonical business columns
- `trip_duration_hours`
- `warning_reason`
- `is_outlier`

It does not persist rejected rows or `error_reason` columns to S3.

## Verification Checklist

Use this checklist after implementation:

- the script exists at the expected path
- the Glue visual job reflects the no-quarantine design
- the processed sink is connected
- invalid rows are rejected but not written to S3
- logs show rejection totals and reasons
- docs and code use the same schema naming convention

## Known Evidence From The Sample Dataset

The 50-row sample parquet already contains invalid cases suitable for test verification:

- 3 rows with `passenger_count <= 0`
- 2 rows with negative `fare_amount`
- 2 rows where `total_amount < fare_amount`
- 1 row with cash payment and positive tip

These cases should be used as a minimum smoke-test set for the transform.
