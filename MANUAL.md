# Manual

Step-by-step setup.

## 1. Prerequisites

- [Docker Desktop](https://docs.docker.com/get-docker/) (includes Docker Compose) — **must be running** before any command below
- Ports `3307`, `5433`, `8080` free on your machine (see Troubleshooting in the README if not)
- The dataset: [Flight Price Dataset of Bangladesh](https://www.kaggle.com/datasets/mahatiratusher/flight-price-dataset-of-bangladesh) (Kaggle)

## 2. Clone the repo

```bash
git clone https://github.com/josephforson285/airflow-flight-price-analysis.git
cd airflow-flight-price-analysis
```

## 3. Get the data

Download the CSV from Kaggle and place it at:

```
include/data/raw/Flight_Price_Dataset_of_Bangladesh.csv
```

Without it, `ingest_csv_to_mysql` fails on a missing file.

## 4. Configure and start

```bash
make init     # generates .env from .env.example with fresh secrets
make build    # first time only — downloads + builds images, a few minutes
make up
make ps       # re-run until every service says "healthy"
```

`make init` auto-generates `FERNET_KEY`, `JWT_SECRET`, and the DB passwords.
It does **not** touch `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` — those
come straight from `.env.example`.  

Then open <http://localhost:8080> and log in with those two values.

## 5. Run the pipeline

Trigger `flight_price_pipeline` from the UI, or:

```bash
docker compose exec -T airflow-scheduler airflow dags trigger flight_price_pipeline
```



## 6. Verify

```bash
make test          # unit + DAG tests
make integration    # end-to-end run against the corrupted fixture
make q SQL="SELECT COUNT(*) FROM fct_flights"
```

Full design rationale and KPI results: [`docs/REPORT.md`](docs/REPORT.md).

## 7. Stop it

```bash
make down     # stop, keep the data
make clean    # stop and wipe all volumes
```
