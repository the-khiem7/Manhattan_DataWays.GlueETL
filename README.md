# Manhattan DataWays GlueETL Jobs

## Primary target use case:

* Source data lives in S3
* A crawler has already created a Glue Data Catalog table
* ETL must clean and validate data
* Output should be split into:
  * **processed** data
  * **quarantine/error** data

---

## Final architecture

Preferred flow:

```text
S3 raw
  -> Glue Crawler
  -> Glue Data Catalog table
  -> Glue ETL Job
      -> valid records -> S3 processed
      -> invalid records -> S3 quarantine
```

Recommended S3 layout:

```text
s3://<bucket>/raw/
s3://<bucket>/processed/
s3://<bucket>/quarantine/
s3://<bucket>/glue-temp/
```

Do **not** overwrite raw data.

---

## Authoring decision

For this use case, prefer:

* **Glue Studio Visual ETL** for the pipeline structure
* **Custom Transform (Python)** for non-trivial validation logic

Reason:

* Visual ETL is easier to build, explain, and demo
* Business rules and data-quality rules are clearer in code than in drag-and-drop transforms
* Notebook/script is only necessary if the pipeline becomes much more code-heavy

---

## Source node decision

Use  **only one source node** :

* `AWS Glue Data Catalog`

Do **not** add a second `Amazon S3` source node for the same dataset if the table already points to S3.

Correct mental model:

* **Crawler** creates metadata
* **Data Catalog table** points to S3 data
* **ETL Job** reads from that table and processes the actual data

---

## Visual ETL node pattern

Recommended node layout:

```text
AWS Glue Data Catalog
  -> Change Schema
  -> Custom Transform (ValidateAndSplit)
      -> SelectFromCollection(valid)
          -> S3 Target (processed)
      -> SelectFromCollection(quarantine)
          -> S3 Target (quarantine)
```

Important:

* Custom transform returns a **DynamicFrameCollection**
* Each output must be selected with **SelectFromCollection**
* Target nodes consume selected outputs, not the raw collection

---

## Change Schema guidance

If datetime columns are already inferred as `timestamp`, do **not** recast them unnecessarily.

Example:

* `tpep_pickup_datetime` = `timestamp`
* `tpep_dropoff_datetime` = `timestamp`

That means:

* type is already correct
* remaining work is  **business validation** , not type conversion

Use `Change Schema` mainly for:

* casting numeric columns
* renaming columns
* dropping unused columns
* cleaning obvious schema mismatches

---

## Validation philosophy

ETL should handle  **two broad classes of issues** :

### 1. Business logic / rule violations

Examples:

* `dropoff < pickup`
* negative duration
* duration greater than a threshold
* passenger count <= 0
* passenger count too large
* distance <= 0
* distance unrealistically large
* negative fare
* `total_amount < fare_amount`
* `total_amount` mismatch
* cash payment but has tip

### 2. Data quality issues

Examples:

* required fields are null
* duplicates
* inconsistent categorical values
* inconsistent formatting
* suspicious outliers

---

## Recommended handling strategy

Do not just `print()` counts and stop.

Use this model:

1. detect issues with masks / conditions
2. label each record with validation metadata
3. split data into valid vs invalid
4. write each output to its own S3 location
5. keep audit columns for debugging and quality monitoring

Recommended output columns:

* `is_valid`
* `error_reason`
* `warning_reason`
* `is_outlier`

---

## Rule taxonomy for the taxi-style dataset

### Hard failures -> quarantine

These should generally mark a record invalid:

* required datetime is null
* required numeric fields are null
* duplicate row
* invalid payment type
* invalid store-and-forward flag
* `dropoff < pickup`
* negative duration
* duration > 24h
* passenger_count <= 0
* passenger_count > 6
* trip_distance <= 0
* trip_distance > 100
* fare_amount < 0
* total_amount < fare_amount
* total mismatch > allowed tolerance
* cash payment with positive tip

### Soft anomalies -> keep but flag

These usually should not be auto-rejected unless business says so:

* fare unusually high
* distance unusually high but still plausible
* duration unusually long but not impossible

Recommended handling:

* keep in processed output
* set `is_outlier = true`
* append reason to `warning_reason`

---

## Null, duplicate, inconsistent, outlier handling

### Nulls

Use hard failure for required columns.

Example required columns:

* pickup datetime
* dropoff datetime
* passenger count
* distance
* fare
* total

Optional columns can be filled or left null depending on business rules.

### Duplicates

If there is no stable trip ID, deduplicate using a business key such as:

* vendorid
* pickup datetime
* dropoff datetime
* passenger count
* trip distance
* fare amount
* total amount

Recommended approach:

* keep first record
* quarantine later duplicates

### Inconsistent data

Normalize values before validating them.

Examples:

* `Cash`, `cash`, `CASH`, `2` -> normalized payment type
* `Y/N` flags should be standardized
* trim whitespace
* uppercase standardized categories

### Outliers

Treat outliers separately from hard validation errors.

Example:

* `fare_amount > 300`
* `trip_distance > 50`
* `trip_duration_hours > 4`

These can become warnings instead of hard failures.

---

## Custom transform contract

The custom transform should:

* accept a `DynamicFrameCollection`
* convert the input frame to Spark DataFrame
* normalize and validate rows
* split into `valid` and `quarantine`
* return a `DynamicFrameCollection`

Canonical output keys:

* `valid`
* `quarantine`

After the transform, use:

* `SelectFromCollection(valid)`
* `SelectFromCollection(quarantine)`

---

## Output schema note

Glue Studio does **not** reliably infer custom transform output schemas automatically for all cases.

Plan to define output schema explicitly for both outputs.

### `valid` output should include

* original cleaned columns
* `trip_duration_hours`
* `is_valid`
* `is_outlier`
* `warning_reason`

### `quarantine` output should include

* original columns
* `trip_duration_hours`
* `error_reason`
* `warning_reason`
* `is_valid`
* `is_outlier`

---

## Recommended sink setup

### Processed sink

* Target: `Amazon S3`
* Path: `s3://<bucket>/processed/...`
* Format: **Parquet**
* Compression: **Snappy** if available
* Partitioning: optional, but recommended for analytics

### Quarantine sink

* Target: `Amazon S3`
* Path: `s3://<bucket>/quarantine/...`
* Format: Parquet or JSON
* Include failure metadata for debugging

Parquet is preferred for analytics and downstream query performance.

---

## Job settings

### During development

* **Job bookmark** : `Disable`

Reason:

* easier to rerun against the full dataset
* easier to debug validation changes

### During incremental / production runs

* **Job bookmark** : `Enable`

Reason:

* avoid reprocessing old source files

Also configure:

* Glue temp directory, e.g. `s3://<bucket>/glue-temp/`
* IAM role with permissions for:
  * reading raw S3
  * writing processed/quarantine S3
  * Glue Catalog access
  * CloudWatch logs

---

## Glue Data Quality position

Glue Data Quality can be added, but it is optional for this phase.

Best split of responsibilities:

* **Custom Transform**
  * cross-column business rules
  * complex validation logic
  * quarantine routing
* **Glue Data Quality**
  * null checks
  * completeness
  * uniqueness
  * standard rule scoring / monitoring
  * anomaly tracking

For the current dataset and learning flow, **Visual ETL + Custom Transform** is the main recommendation.

---

## Canonical implementation direction

### Use Visual ETL when

* you want a clear DAG / pipeline view
* the ETL is part of a demo, workshop, or architecture explanation
* the job is mostly standard source -> transform -> sink

### Use Notebook / Script when

* logic becomes heavily code-driven
* you need iterative debugging
* you need advanced PySpark or reusable code modules

For this conversation's use case:

* **Start with Visual ETL**
* **Use custom Python transform for validation**

---

## Minimal custom transform responsibilities

The custom transform should implement the following stages:

1. cast and normalize columns
2. derive `trip_duration_hours`
3. detect duplicates
4. define masks for:
   * null required
   * duplicate
   * inconsistent categorical values
   * logic time
   * business rule violations
   * soft outliers
5. build `error_reason`
6. build `warning_reason`
7. set `is_valid` / `is_outlier`
8. split into `valid_df` and `quarantine_df`
9. return both outputs as a `DynamicFrameCollection`

---

## Summary of final decisions

* Read from  **Glue Data Catalog** , not directly from a second S3 source node
* Keep datetime columns as `timestamp` if they are already inferred correctly
* Use **Change Schema** only for cleanup, not unnecessary recasting
* Put complex validation into a **Custom Transform**
* Split outputs into:
  * `processed`
  * `quarantine`
* Use **SelectFromCollection** after the custom node
* Prefer **Parquet** for processed output
* Keep **job bookmarks disabled** while developing
* Enable bookmarks later for incremental production runs

---

## One-line recommendation

For this project, the cleanest setup is:

```text
Glue Data Catalog -> Change Schema -> Custom Transform (validate + split) -> S3 processed / S3 quarantine
```
