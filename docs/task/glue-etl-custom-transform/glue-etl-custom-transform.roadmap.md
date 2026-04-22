# Glue ETL Custom Transform Roadmap

## Task Summary

Build the custom-transform portion of the Manhattan DataWays `Glue ETL Job` so that raw yellow taxi trip data from the Glue Catalog is cleaned, validated, and written only to the processed zone.

## Resume Rules

Use this document as the first checkpoint when resuming work.

Before writing code, verify these files again:

- [README.md](/d:/SourceCode/FirstCloudJourney/Manhattan_DataWays.GlueETL/README.md)
- [DataSchema.md](/d:/SourceCode/FirstCloudJourney/Manhattan_DataWays.GlueETL/DataSchema.md)
- [Architecture.png](/d:/SourceCode/FirstCloudJourney/Manhattan_DataWays.GlueETL/Architecture.png)
- [AWS Glue Jobs.png](/d:/SourceCode/FirstCloudJourney/Manhattan_DataWays.GlueETL/docs/img/glue/AWS%20Glue%20Jobs.png)
- [data/taxi_data_50_rows.parquet](/d:/SourceCode/FirstCloudJourney/Manhattan_DataWays.GlueETL/data/taxi_data_50_rows.parquet)

If any of those change, update this roadmap before continuing implementation.

## Current Status Snapshot

Status as of 2026-04-22:

- overall architecture is defined
- raw sample parquet is available
- schema mapping document is available
- Glue visual job already includes `Data Catalog`, `Change Schema`, `Custom Transform`, and `Select From Collection`
- repository does not yet contain a tracked custom transform script
- repository does not yet contain task documentation for this topic
- legacy README still mentions quarantine output
- current task scope explicitly removes quarantine persistence

## Scope Decisions

Accepted decisions:

- source is `AWS Glue Data Catalog`, not a second direct S3 source node
- this task covers the `Glue ETL Job` stage only
- output target is processed data only
- invalid rows are rejected in-memory during ETL
- invalid rows are not written to a quarantine S3 path
- rejection visibility should come from logs, counters, and optional job metrics

Open implementation decisions:

- whether column-name normalization happens mostly in `Change Schema` or in Python
- whether the custom transform should still return a `DynamicFrameCollection` with a single named output, or return a single frame directly if the visual job is simplified
- which rejection-reason fields, if any, should be retained in the processed output for auditability

## Work Breakdown

### Phase 1: Baseline and contract

Status: `completed`

Tasks:

- create `glue_jobs/yellow_taxitrip_custom_transform.py`
- define function signature compatible with Glue custom transform usage
- document the expected input frame name from the visual job
- document the output contract for the no-quarantine design

Exit criteria:

- a runnable script skeleton exists
- the transform contract is explicit in code comments and in the use guide

### Phase 2: Schema normalization

Status: `completed`

Tasks:

- reconcile raw parquet column names with `DataSchema.md`
- decide the final canonical column names inside the transform
- ensure timestamp and numeric types are stable after `Change Schema`
- handle optional fields consistently

Known mismatch to account for:

- parquet uses `VendorID`, `RatecodeID`, `PULocationID`, `DOLocationID`, `Airport_fee`
- schema doc uses lowercase target-style equivalents

Exit criteria:

- one canonical schema is chosen
- the code and docs use the same names consistently

### Phase 3: Validation and cleaning logic

Status: `completed`

Tasks:

- derive trip duration from pickup and dropoff timestamps
- normalize categorical values such as `store_and_fwd_flag`
- implement hard-failure checks for invalid rows
- decide whether soft anomalies are dropped or retained with warning metadata

Minimum hard-failure candidates from current evidence:

- `passenger_count <= 0`
- `fare_amount < 0`
- `total_amount < fare_amount`
- `cash payment` with positive tip
- required field nulls

Possible follow-up rules:

- `dropoff < pickup`
- excessive duration threshold
- invalid payment type domain
- invalid location identifiers

Exit criteria:

- rules are implemented in code
- rules are documented in the use guide
- sample parquet can be used to demonstrate accepted vs rejected rows

### Phase 4: Output and observability

Status: `pending`

Tasks:

- connect the accepted output to the processed S3 target
- remove all quarantine-specific design assumptions from code and docs
- emit rejection counts and reason breakdowns to logs
- confirm how the visual job should look after simplification

Exit criteria:

- the job writes one processed dataset
- invalid records are not persisted to another S3 path
- operational visibility exists through logs or counters

### Phase 5: Validation and documentation closeout

Status: `pending`

Tasks:

- test against `data/taxi_data_50_rows.parquet`
- capture expected behavior for sample invalid cases
- update this roadmap with actual progress and implementation notes
- finalize the use guide after the script is stable

Exit criteria:

- another engineer can resume from the docs alone
- docs match the current code and visual job

## Suggested Execution Order

1. Create script skeleton
2. Lock canonical column names
3. Implement rule helpers and rejection logic
4. Decide single-output transform contract
5. Attach processed sink in Glue
6. Verify against sample parquet
7. Update docs to reflect final behavior

## Risks

- `README.md` still encodes the older quarantine pattern and can mislead implementation
- Glue Studio custom transform behavior may differ depending on whether the node returns a frame or a collection
- schema-name mismatches between parquet and mapping doc can cause broken field references
- local repo currently lacks the actual transform script, so code and docs can drift unless updated together

## Tracking Log

Use this section for resumable updates. Append new entries instead of rewriting history.

### 2026-04-22

- Confirmed that the architecture places this work at the `Glue ETL Job` stage.
- Confirmed from the Glue screenshot that the current visual job stops at `Select From Collection`.
- Confirmed that `glue_jobs/` is present but has no tracked script yet.
- Confirmed that `README.md` still describes quarantine output, but current task scope removes that output.
- Confirmed from the 50-row parquet sample that real invalid examples already exist for validation testing.
- Created `glue_jobs/yellow_taxitrip_custom_transform.py` as the initial script skeleton.
- Declared the current transform contract with a single `processed` output and no quarantine sink.
- Chose lowercase target-style field names as the canonical internal schema.
- Added raw-to-canonical field mapping and basic type coercion so the transform can tolerate either raw parquet names or renamed Glue columns.
- Added record enrichment with `trip_duration_hours`, `is_valid`, `error_reason`, `warning_reason`, and `is_outlier`.
- Implemented hard-failure checks for required fields, duplicates, categorical validity, time logic, passenger count, distance, fare, and cash-tip inconsistency.
