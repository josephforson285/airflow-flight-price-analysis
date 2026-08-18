# Manual

Step-by-step setup, not covered in full by the README.

## 1. Prerequisites

- Docker + Docker Compose
- The dataset: [Flight Price Dataset of Bangladesh](https://www.kaggle.com/datasets/mahatiratusher/flight-price-dataset-of-bangladesh) (Kaggle)

## 2. Get the data

Download the CSV from Kaggle and place it at:

```
include/data/raw/Flight_Price_Dataset_of_Bangladesh.csv
```

This path is not in git (`include/data/raw/` is gitignored — bulk source data).
Without it, `ingest_csv_to_mysql` fails on a missing file.

## 3. Configure and start

```bash
make init     # generates .env from .env.example with fresh secrets
make build
make up
make ps       # wait until every service reports healthy
```

`make init` auto-generates `FERNET_KEY`, `JWT_SECRET`, and the DB passwords.
It does **not** touch `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` — those
come straight from `.env.example`. Open `.env` and check those two values
before logging in at <http://localhost:8080>.

## 4. Run the pipeline

Trigger `flight_price_pipeline` from the UI, or:

```bash
docker compose exec -T airflow-scheduler airflow dags trigger flight_price_pipeline
```

~14 tasks, ~15–20s against the full 57,000-row dataset.

## 5. Verify

```bash
make test          # unit + DAG tests
make integration    # end-to-end run against the corrupted fixture
make q SQL="SELECT COUNT(*) FROM fct_flights"
```

Full design rationale and KPI results: [`docs/REPORT.md`](docs/REPORT.md).
