# Glue ETL Custom Transform Introduction

## Objective

Implement the `Glue ETL Job` step in the Manhattan DataWays pipeline shown in [Architecture.png](/d:/SourceCode/FirstCloudJourney/Manhattan_DataWays.GlueETL/Architecture.png). The job must read curated metadata from `AWS Glue Catalog`, apply custom validation and cleaning logic to raw NYC yellow taxi trip data, and write only accepted records to the processed zone.

This task is intentionally limited to the `Glue ETL Job` stage in the sequence:

`AWS Glue Crawler -> AWS Glue Catalog -> Glue ETL Job`

## Current Situation

The repository currently contains:

- Architecture and Glue screenshots
- Raw sample data in parquet format
- A schema mapping document
- A `README.md` that still describes a `processed + quarantine` split
- An empty `glue_jobs/` folder
- An empty task folder at `docs/task/glue-etl-custom-transform/`

There is no tracked Glue custom-transform Python script in the repository yet.

## Confirmed Visual Job State

The latest Glue Job screenshot at [AWS Glue Jobs.png](/d:/SourceCode/FirstCloudJourney/Manhattan_DataWays.GlueETL/docs/img/glue/AWS%20Glue%20Jobs.png) shows this visual flow:

`AWS Glue Data Catalog -> Change Schema -> Custom Transform -> Select From Collection`

The visual job does not yet show:

- a processed S3 sink
- a finalized custom transform script
- a revised one-output design after removing quarantine handling

## Required Direction Change

`README.md` describes a design that writes invalid records into a quarantine S3 path. That is no longer the target.

This task must use the updated rule:

- do not persist invalid records to a separate quarantine S3 location
- keep only valid cleaned records for processed output
- reject invalid rows inside the ETL logic
- capture rejection counts and reasons through logs or metrics instead of writing a second dataset

This scope change must be treated as the source of truth for the new implementation and for all task documents in this folder.

## Data Understanding

The task should be based on both:

- [DataSchema.md](/d:/SourceCode/FirstCloudJourney/Manhattan_DataWays.GlueETL/DataSchema.md)
- [data/taxi_data_50_rows.parquet](/d:/SourceCode/FirstCloudJourney/Manhattan_DataWays.GlueETL/data/taxi_data_50_rows.parquet)

### Important schema reality check

The sample parquet file does not fully match the normalized naming style shown in `DataSchema.md`.

Observed raw parquet column examples:

- `VendorID`
- `RatecodeID`
- `PULocationID`
- `DOLocationID`
- `Airport_fee`

`DataSchema.md` presents lowercase target-style names such as:

- `vendorid`
- `ratecodeid`
- `pulocationid`
- `dolocationid`
- `airport_fee`

This means the ETL implementation must decide explicitly where normalization happens:

- either in `Change Schema`
- or inside the custom transform
- or with a deliberate split between both

The sample parquet file contains 50 rows and includes realistic invalid examples that the transform should handle, including:

- `passenger_count <= 0` in 3 rows
- `fare_amount < 0` in 2 rows
- `total_amount < fare_amount` in 2 rows
- `payment_type = cash` with positive tip in 1 row

No duplicate rows were observed in the 50-row sample using a business-key style comparison across the main trip fields.

## Implementation Direction

The recommended implementation direction is:

1. Read from `AWS Glue Data Catalog`
2. Apply `Change Schema` only where it reduces ambiguity
3. Implement the business rules in a Python custom transform
4. Return only records that pass validation for downstream processed output
5. Log rejected-record counters and reasons instead of writing a quarantine dataset

Target implementation file:

- `glue_jobs/yellow_taxitrip_custom_transform.py`

## Deliverables For This Task Group

This documentation set exists to support implementation and resume work without prior chat memory:

- `glue-etl-custom-transform.introduction.md`
- `glue-etl-custom-transform.roadmap.md`
- `glue-etl-custom-transform.useguide.md`

## Definition Of Done

This task group is complete when:

- the custom transform script exists and matches the current no-quarantine requirement
- the Glue visual job is aligned with a single processed output path
- validation rules are documented and reproducible
- resume status is visible from the roadmap document alone
- the use guide is sufficient for another engineer to run or continue the task without prior conversation context
