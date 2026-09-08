# Project: NYC Taxi Data Engineering Pipeline

## What this is
An orchestrated ELT pipeline built as a portfolio project for data engineer job applications.
Extracts NYC Yellow Taxi trip data monthly, loads it into Snowflake, transforms it with dbt,
all coordinated by Airflow (via Astro CLI, running in Docker).

## Current architecture
NYC TLC (public parquet files) → Airflow (extract → stage → load) → Snowflake raw schema
→ dbt via Cosmos (staging → incremental fact table → aggregate marts) → Snowflake staging/marts schemas

## Environment
- Windows ARM64 machine — confirmed Airflow/Docker run natively, no x86 emulation needed
- Astro CLI running Airflow locally in Docker (`astro dev start` from project root)
- dbt Core + dbt-snowflake installed locally (had to add Python Scripts folder to PATH manually)
- Snowflake trial account: warehouse `taxi_wh`, database `nyc_taxi`, schemas `raw`/`staging`/`marts`
- Service role/user for the pipeline: `taxi_pipeline_role` / `taxi_pipeline_svc`
  (never use personal ACCOUNTADMIN login for the pipeline itself)

## Project structure
- `dags/nyc_taxi_full_pipeline.py` — main DAG (extract/stage/load Python tasks + DbtTaskGroup via Cosmos)
- `dags/nyc_taxi_extract_load.py` — OLDER, simpler DAG (extract/load only, no dbt). Currently paused.
  Keep paused — running both DAGs simultaneously caused a duplicate-load bug once already.
- `include/nyc_taxi_dbt/` — THE dbt project (models/staging, models/marts, seeds, tests, macros).
  Single source of truth. It lives under `include/` because that is one of the few directories the
  Astro CLI bind-mounts into the Airflow container, which is where Cosmos reads it from
  (`DBT_PROJECT_PATH` in the main DAG).
  Run local dbt commands against it explicitly: `dbt test --project-dir include/nyc_taxi_dbt`
  After changing models, `astro dev restart`.
  **There used to be a second, identical copy at the repo root requiring a manual `xcopy` re-sync
  after every change. It was removed on 2026-09-08. Do NOT reintroduce it** — the two copies
  drifting silently (Airflow running a stale model while local dbt showed the new one) is exactly
  the failure mode the removal prevents.

## Key models
- `stg_yellow_tripdata` (view): casts raw types, fixes timestamps (raw parquet had microsecond-epoch
  integers, not real timestamps — fixed with `TO_TIMESTAMP_NTZ(col, 6)`)
- `fct_trips` (INCREMENTAL — important): filters out negative fares and out-of-range dates
  (`>= 2020-01-01`, `<= current_timestamp()`), then appends only rows newer than the existing
  max `pickup_datetime` on each run. Do NOT change this back to a plain `table` materialization —
  that was an earlier bug that would silently discard prior months on every backfill run.
- `agg_trips_by_day`, `agg_revenue_by_zone` (tables): built on top of `fct_trips`

## Known data quirks (intentional, not bugs)
- ~1.2% of trips have negative `total_amount` (refunds/corrections) — filtered in `fct_trips`,
  left untouched in staging/raw. The `assert_no_negative_fares` test is set to WARN not ERROR
  since this is a known, handled condition.
- A handful of trips per month have corrupted pickup dates (e.g. stamped in 2002, 2009) —
  filtered by the date-range bound in `fct_trips`.
- NYC TLC publishes data with a ~2 month lag. The extract task raises a clear RuntimeError on
  HTTP 403/404 rather than crashing ambiguously — this is expected behavior for unpublished months.

## Current schedule state
- DAG: `nyc_taxi_full_pipeline`, schedule `@monthly`, `start_date=2026-03-01`, `catchup=True`, `max_active_runs=1`
- Successfully backfilled: March, April, May 2026 (+ January 2024 from earlier manual testing)
- June 2026 run FAILED as expected (TLC hadn't published it yet) — this is documented, not a bug to fix
- **DAG is currently PAUSED.** Unpause when TLC has published more recent months, or extend
  `start_date`/`schedule` if picking this project back up later.

## Snowflake gotcha to remember
Objects created under one role (e.g. via `CREATE OR REPLACE` run as ACCOUNTADMIN) are NOT
automatically visible to other roles, even other admin roles, unless future-grants were set up
first. Fixed once already with:
```sql
GRANT ALL ON FUTURE TABLES IN DATABASE nyc_taxi TO ROLE taxi_pipeline_role;
GRANT ALL ON FUTURE VIEWS IN DATABASE nyc_taxi TO ROLE taxi_pipeline_role;
```
If a "does not exist or not authorized" error shows up again, this is almost certainly why.

## GitHub repo
https://github.com/vishnukanchi9/nyc-taxi-data-engineering-pipeline
Public. Contains the full runnable Astro project: `dags/`, `include/nyc_taxi_dbt/` (the dbt project,
minus target/logs), Dockerfile, packages.txt, requirements.txt, `.astro/`, `tests/`, `.gitignore`,
`.gitattributes`, and the README with architecture diagram, lineage screenshot (`dbt-dag.png`) and
the "Real Problems Solved" section written for interviews.
This file (CLAUDE.md) is committed as the project context doc.
Local git was only initialised on 2026-09-08; before that the repo was populated by web upload.

## What's NOT done yet / possible next steps
- No surrogate key for trips — full-row matching is used for dedup, which is fragile
- Credentials are in Airflow's connection store (metadata DB), not a real secrets backend
- No alerting on task failure (Slack/email)
- Only Yellow Taxi data — Green Taxi and FHV (Uber/Lyft) would be natural extensions