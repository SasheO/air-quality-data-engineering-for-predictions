# Air Quality Historical

## Overview
Fetches hourly air pollution data from the OpenWeather Air Pollution History API for each city in the `city_coordinates` table, covering the entirety of the previous calendar month. Parsed rows are bulk-inserted into `air_quality_datasets.raw` in ClickHouse, where incremental dbt models downstream transform them into features.

## Schedule
- Cron: `0 6 1 * *` — 06:00 UTC on the 1st of every month.
- Catchup enabled from `2021-02-01`; `max_active_runs=1`.
- Time window per run: `[00:00:00 UTC on the 1st of the previous month, 23:59:59 UTC on the last day of the previous month]`.

## DAG Task Dependencies
create_databases_if_not_exists

create_tables_if_not_exists
refresh_city_coordinates_table
get_city_coordinates
write_previous_month_data


## Tasks

### `create_databases_if_not_exists`
SQLExecuteQueryOperator. Runs `sql/create_database.sql` against ClickHouse to create `air_quality_datasets` if it does not exist.

### `create_tables_if_not_exists`
SQLExecuteQueryOperator. Runs `sql/create_raw_data_table.sql` (split into statements) to create the `raw` and `city_coordinates` tables if they do not exist.

### `refresh_city_coordinates_table`
Reads `include/csv/latitudes_and_longitudes.csv`, casts `*_coordinate` columns to `float`, and bulk-inserts all rows into `air_quality_datasets.city_coordinates`.

### `get_city_coordinates`
Runs `sql/get_deduplicated_city_coordinates.sql` and returns a list of dicts (one per city) with the columns needed for the API call and row building (e.g. `city_coordinates_id`, `city`, `country`, `latitude_coordinate`, `longitude_coordinate`). Output is XCom'd to `write_previous_month_data`.

### `write_previous_month_data`
For each city:
1. Computes the previous-month Unix window (`start`, `end`).
2. Calls `http://api.openweathermap.org/data/2.5/air_pollution/history` with `lat`, `lon`, `start`, `end`, `appid`.
3. Parses each hourly entry via `build_rows` into a tuple matching `RAW_TABLE_COLUMNS`; entries with empty `components` are skipped.
4. Collects all rows across all cities and performs one bulk insert into `air_quality_datasets.raw`. Skips the insert with a warning if no rows were produced.

## Required Variables
| Variable | Description |
|---|---|
| `OPENWEATHER_API_SECRET_KEY` | OpenWeather API key used for history requests. Set as an encrypted Airflow Variable. |

Also required (as Airflow Connections):
| Connection | Description |
|---|---|
| `clickhouse_conn` | ClickHouse connection used by all SQL and insert tasks. |