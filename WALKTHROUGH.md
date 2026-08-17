# Demo Walkthrough

Personal notes for showing this pipeline. Every command here has been run.

**Total demo time: ~10 min.** If you only have 3, do §3 and §5.

---

## 0. Before the demo

```bash
cd ~/Desktop/bear/ML/airflow-flight-price-analysis
make up          # ~40s if images are built; check everything says healthy
make ps
```

Open <http://localhost:8080> — login `airflow` / `airflow`.

> If ports clash: your machine runs a **local** MySQL on 3306 and PostgreSQL on
> 5432. The containers use **3307 / 5433 / 8080** on purpose. Nothing collides.

### How fresh do you want to be?

Pick the lightest one that does the job — heavier is slower, not better.

| Level | Command | Time | Use when |
|---|---|---|---|
| **1. New run** | just trigger again | ~15s | Between takes. Full refresh replaces everything anyway. |
| **2. Clear run history** | see below | ~5s | The UI is cluttered with past runs, or a failed run is holding the slot |
| **3. Wipe the databases** | `make clean && make up` | ~1 min | You want the tables provably empty before you start |
| **4. Everything, incl. image** | `make nuke && make build && make up` | ~4 min | Proving a cold clone works |

**Level 2 — clear run history** (the one you'll actually use):

```bash
docker compose exec -T airflow-scheduler airflow dags delete flight_price_pipeline -y
docker compose exec -T airflow-scheduler airflow dags reserialize
docker compose exec -T airflow-scheduler airflow dags unpause flight_price_pipeline
```

This wipes Airflow's *run history* only — the databases keep their data. It's
also the fix when a run won't start because a previously failed one still holds
the single run slot.

**Level 3 — prove the tables are empty**, which demos well:

```bash
make clean && make up
make q SQL="SELECT COUNT(*) FROM fct_flights"   # table doesn't exist yet — that IS the point
```

Then trigger and watch it build from nothing in ~14 seconds.

### Give each demo run a name you can point at

```bash
docker compose exec -T airflow-scheduler \
  airflow dags trigger flight_price_pipeline --run-id demo_monday_1
```

That name becomes the `batch_id` stamped on every row it produces (see §2.5),
so you can say "these 57,000 rows came from *that* run" and then show it.

---

## 1. Show the shape first (30s)

In the UI: **DAGs → flight_price_pipeline → Graph**.

Point at three things:

- **Two DDL branches run in parallel** — the analytics schema doesn't depend on
  any staging work, so it builds alongside it.
- **`assert_reject_rate` sits between validation and the build** — it's a gate,
  and nothing downstream runs if it fails.
- **The four KPI tasks fan out** — they aggregate the same table independently.
  The DAG ends on an *assertion*, not a write.

> Line to use: *"a pipeline that ends on a load can go green having written
> nothing — the last task is what makes success mean something."*

---

## 2. Run it (1 min)

UI: **Trigger** ▶ on the DAG. Or:

```bash
docker compose exec -T airflow-scheduler \
  airflow dags trigger flight_price_pipeline --run-id demo_$(date +%s)
```

Watch the Graph go green. **57,000 rows, ~14 seconds, 14 tasks.**

Check it from the shell:

```bash
docker compose exec -T airflow-scheduler \
  airflow tasks states-for-dag-run flight_price_pipeline <run-id> -o plain
```

---

## 2.5 Lineage — "where did this row come from?" (1 min)

Every row in every table carries a `batch_id`. It is **the Airflow `run_id`,
copied verbatim** — so the name you pass to `--run-id` is the name stamped on
the data.

```bash
make psql
```
```sql
SELECT batch_id, COUNT(*), MIN(loaded_at) FROM fct_flights GROUP BY batch_id;
```

```
 batch_id    | count |         min
-------------+-------+----------------------------
 docker_trim | 57000 | 2026-08-17 19:15:15
```

Same id appears in `raw_flight_prices`, `stg_flights` and `fct_flights` — one
run's fingerprint, traceable end to end across both databases.

**The question someone will ask: why only one batch?**

Because every layer is a **full refresh** — each run deletes and reloads rather
than appending. So `batch_id` is a *lineage stamp*, not a history partition:
it answers "which run produced this row", not "show me last Tuesday's load".

That's a deliberate choice, and worth saying out loud:

> *"The source is one static extract, so accumulating batches would just
> duplicate the same 57,000 rows. Full refresh is also what makes every task
> safe to re-run — clear one and re-run it and the counts come out identical
> rather than doubling."*

If this became a daily feed, `batch_id` is already the column you'd partition
on — the design doesn't have to change, only the delete scope.

Trace a single row back to its line in the CSV:

```sql
SELECT batch_id, raw_row_num, airline, source_code, destination_code
FROM fct_flights ORDER BY raw_row_num LIMIT 3;
```

`raw_row_num` is the 1-based line number in the source file, so any row —
including any quarantined one — points back at the exact CSV line it came from.

---

## 3. ⭐ The money shot — the gate blocking bad data

This is the bit worth showing. Point the *same DAG* at a deliberately corrupted
file, at the default 5% threshold:

```bash
docker compose exec -T airflow-scheduler airflow dags trigger flight_price_pipeline \
  --run-id badrun_$(date +%s) \
  --conf '{"source_csv_path": "/opt/airflow/include/data/fixtures/corrupted_sample.csv"}'
```

In the UI, `assert_reject_rate` goes **red** and everything downstream never
runs. Then show *why*:

```bash
make mysql
```
```sql
SELECT reason_code, COUNT(DISTINCT raw_row_num) AS rows_hit
FROM rejects_flight_prices GROUP BY reason_code ORDER BY 1;
```

12 reason codes, 16 of 32 rows caught. Then the punchline:

```sql
SELECT COUNT(*) FROM stg_flights;    -- still the PREVIOUS good data
```

> Line to use: *"the gate blocked it **before** the staging build ran, so the
> bad batch never overwrote the good data. It doesn't just detect — it
> prevents."*

Show a single quarantined row with its evidence:

```sql
SELECT raw_row_num, reason_code, LEFT(reason_detail, 50) AS detail
FROM rejects_flight_prices ORDER BY raw_row_num LIMIT 8;
```

`exit` to leave the shell.

---

## 4. Why the fixture exists (30s, if they ask)

The real data is **too clean to test the error handling** — zero nulls, zero
duplicates, zero negative fares. Every validation rule passes against it, so
the quarantine path would never execute.

> Line to use: *"untested code that merely looks like protection. So I built a
> file that breaks every rule on purpose — and it immediately found two real
> bugs in my own validation."*

---

## 5. ⭐ The findings — this is the analysis, not the plumbing

```bash
make psql
```

**The headline — seasonal pricing:**
```sql
SELECT seasonality, bookings, avg_total_fare_bdt, vs_regular_pct
FROM kpi_seasonal_variation ORDER BY avg_total_fare_bdt DESC;
```
Hajj **+42.7%**, Eid **+34.5%**, Winter **+17.0%** over baseline. Large, clean,
well-ordered — demand concentrated into immovable travel windows.

**The anomaly I found before writing any code:**
```sql
SELECT COUNT(*) AS markup_rows,
       ROUND(100.0*COUNT(*)/(SELECT COUNT(*) FROM fct_flights),2) AS pct,
       MIN(fare_variance_pct), MAX(fare_variance_pct)
FROM fct_flights WHERE has_fare_discrepancy;
```
2,522 rows (4.42%) where `total = (base + tax) × 1.2` **exactly** — min = max.

> Line to use: *"deterministic, not corruption. Dirty data or an unstated
> surcharge rule — can't tell from the data. So the pipeline refuses to pick:
> it keeps both values and flags the row. In a job you'd ask the data owner."*

**Where I pushed back on the data:**
```sql
SELECT COUNT(*) AS rows,
       COUNT(DISTINCT (airline, source_code, destination_code, departure_at)) AS distinct_flights
FROM fct_flights;
```
57,000 = 57,000. **No flight appears twice.** So "bookings" is the brief's word,
not the data's — there's no booking id or seat count. That's why the sum column
is `total_fare_bdt` and **not** `revenue_bdt`: revenue needs seats sold.

`\q` to exit.

---

## 6. Engineering quality (2 min, for a technical audience)

```bash
make test          # 41 tests
make integration   # 14 assertions against real databases
make lint
```

Then the one that makes the point:

```bash
docker run --rm -v "$PWD":/workspace -w /workspace python:3.13-slim sh -c \
  'pip install -q -e . pytest && python -m pytest tests/unit'
```

**32 tests in 0.03s with Airflow not installed at all.** The pipeline logic
imports no Airflow — database access is injected as a callable — so CI checks
logic in seconds instead of building a 3 GB image.

**Prove re-runnability** (someone always asks):

```bash
# run the DAG twice; counts stay identical, they don't double
docker compose exec -T postgres-analytics sh -c \
  'psql -tAX -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT COUNT(*) FROM fct_flights"'
```

---

## 7. Questions I've been asked

**"Why two different databases for 57k rows?"**
Nobody would choose it. It's the brief's constraint, and it buys one real
lesson: the cross-engine hop is where pipelines break.

**"How do you know the tests actually work?"**
I broke them on purpose. Added a phantom column to the schema — the drift test
failed and named it. Forced a division-by-zero mid-rebuild — the previous KPI
data survived intact.

**"What would you do differently?"**
Schema migrations for `fct_flights`. `CREATE TABLE IF NOT EXISTS` can't evolve a
column — it bit me once, and the fact table still has no migration path.

**"Is this dbt?"**
No, and deliberately. The brief lists five technologies and dbt isn't one. I
built it, then reverted it — it's in the git history at `d0f50ad`.

---

## 8. If something breaks mid-demo

| Symptom | Fix |
|---|---|
| DAG missing from UI | `make dag-test` — shows import errors the UI hides |
| Run stuck, tasks never start | A previous failed run holds the slot (`max_active_runs=1`). `airflow dags delete flight_price_pipeline -y`, then reserialize + unpause |
| Port already allocated | Your local MySQL/Postgres. Change ports in `.env` |
| Fresh clone won't start | `make init` — generates the Fernet key and JWT secret; plain `cp .env.example .env` leaves placeholders |

Reset to a known-good state:

```bash
make clean && make up     # wipes volumes, ~1 min, then re-trigger
```

---

## Numbers to have in your head

```
57,000 rows · 17 columns · 14 MB · 24 airlines · 152 routes
14 tasks · ~14 seconds · 12 validation rules
2,522 markup rows (4.42%) · Hajj +42.7% · Regular baseline 68,077 BDT
41 tests · 32 of them without Airflow in 0.03s
```
