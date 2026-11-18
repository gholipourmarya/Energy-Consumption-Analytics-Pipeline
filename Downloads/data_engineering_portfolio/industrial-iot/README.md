# Industrial IoT Sensor Pipeline

## Overview
Monitor sensor readings and flag abnormal machine measurements. This is a runnable **starter project** using synthetic data, Python and SQLite.

## Business use case
Monitor sensor readings and flag abnormal machine measurements.

## Data quality rules
The CSV must include `event_id`, `machine_id`, `temperature_c`, `vibration_mm_s`, and `pressure_bar`. Both IDs must be present in each row. Measurements must be finite numbers, and pressure must be nonnegative. A temperature above 85 °C or vibration above 6 mm/s triggers an alert.

Invalid rows are saved in `output/rejected.json`. If every row is rejected, the pipeline still writes the rejection report, an empty clean file, and an empty summary and warehouse table.

## Architecture
```text
Synthetic CSV -> Python validation and transformation -> JSONL + SQLite -> SQL aggregates -> JSON report
```

## Quick start
Requires Python 3.10+. No third-party packages required. From this repository's root:

```bash
python -m src.pipeline
python -m unittest discover -s tests -v
```

Results are written to `output/`: `clean.jsonl`, `rejected.json`, `warehouse.db`, and `summary.json`.

## SQL
Open `output/warehouse.db` with any SQLite client and run `sql/analysis.sql`.

## Repository structure
- `data/input.csv`: small synthetic dataset
- `src/pipeline.py`: ETL pipeline and aggregation
- `tests/test_pipeline.py`: runnable unit/integration smoke tests
- `sql/analysis.sql`: reporting query
- `output/`: generated artifacts (gitignored)

## Next steps for a production-style portfolio
- Replace the CSV with a public dataset or API.
- Add PostgreSQL, Docker Compose and Airflow scheduling.
- Add idempotent incremental loading, schema validation and monitoring.
- Add dashboards, CI and an architecture diagram.
- For streaming use cases, add Kafka producers/consumers.

## Limitations
This educational prototype uses synthetic sensor measurements and rule-based alerts.

## Status
Runnable local MVP.
