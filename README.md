# GA4 Intraday Snapshot

## Overview  

This dbt model captures **daily snapshots** of GA4 *intraday* export tables (broken down by hour) before they’re deleted and replaced by the daily export. The goal is to compare **intraday vs daily** data to understand the completeness of near-real-time metrics. GA4 documentation notes that intraday tables can miss late-arriving events, so this snapshot lets us quantify those gaps, enabling analysis of **data latency and completeness** compared to GA4’s final daily exports.

## How It Works  

- **Sources:** GA4 datasets for each brand are declared in [`models/ga4/_sources.yml`](models/ga4/_sources.yml).  
- **Model:** [`models/ga4/rpt_intraday_hourly.sql`](models/ga4/rpt_intraday_hourly.sql) unions all brands using a **Jinja loop**, adding a `brand` column for identification.  
- **Scheduling:** Runs daily at **06:00 UTC (12:30 AM ET)** via dbt Cloud.  
- **Logic:**  
  - Selects yesterday’s intraday tables.  
  - Converts timestamps to **America/New_York** and truncates to the hour.  
  - Materialized as an **incremental table** with `insert_overwrite`, so each run overwrites only yesterday’s partition.
