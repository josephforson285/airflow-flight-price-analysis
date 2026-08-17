#!/usr/bin/env bash
#
# End-to-end integration test.
#
# The unit suite proves the logic and the DAG tests prove the shape, but
# neither ever runs the pipeline against a real database. This does: it starts
# the full stack, runs the real DAG against the deliberately corrupted fixture,
# and asserts the quarantine and staging behaviour that the report claims.
#
# Lives in a script rather than inline in the CI workflow so it can be run
# locally, unchanged, by anyone debugging a CI failure.
#
#   ./scripts/integration_test.sh
#
set -euo pipefail

RUN_ID="integration_$$"
FIXTURE="/opt/airflow/include/data/fixtures/corrupted_sample.csv"
TIMEOUT_SECS="${TIMEOUT_SECS:-300}"

# The fixture is 48% bad by design, so the default 5% gate would (correctly)
# block it. We raise the threshold to let the run proceed and exercise the
# whole pipeline; a separate assertion below covers the gate itself.
RELAXED_THRESHOLD=0.6

cd "$(dirname "$0")/.."

log()  { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
fail() { printf '\033[31mFAIL: %s\033[0m\n' "$*" >&2; exit 1; }

airflow_cmd() { docker compose exec -T airflow-scheduler airflow "$@"; }

mysql_scalar() {
  docker compose exec -T mysql-staging sh -c \
    'exec mysql -N -B -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE" -e "'"$1"'"' 2>/dev/null
}

pg_scalar() {
  docker compose exec -T postgres-analytics sh -c \
    'exec psql -tAX -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "'"$1"'"' 2>/dev/null
}

assert_eq() {
  local actual="$1" expected="$2" what="$3"
  actual="$(printf '%s' "$actual" | tr -d '[:space:]')"
  if [ "$actual" != "$expected" ]; then
    fail "$what: expected $expected, got '$actual'"
  fi
  printf '    ok  %-42s %s\n' "$what" "$actual"
}

# Wait for every task in a run to reach a terminal state.
wait_for_run() {
  local run_id="$1" waited=0
  while [ "$waited" -lt "$TIMEOUT_SECS" ]; do
    local states
    states="$(airflow_cmd tasks states-for-dag-run flight_price_pipeline "$run_id" -o plain 2>/dev/null | tail -n +2)"
    if ! printf '%s' "$states" | grep -qE '(running|queued|scheduled|^\s*$)'; then
      # no task still in flight; also require none left blank (unscheduled)
      if [ "$(printf '%s\n' "$states" | grep -cE 'success|failed|up_for_retry|upstream_failed|skipped')" -ge 14 ]; then
        return 0
      fi
    fi
    sleep 5
    waited=$((waited + 5))
  done
  printf '%s\n' "$states" >&2
  fail "run $run_id did not finish within ${TIMEOUT_SECS}s"
}

count_state() {
  airflow_cmd tasks states-for-dag-run flight_price_pipeline "$1" -o plain 2>/dev/null \
    | tail -n +2 | grep -cE "\b$2\b" || true
}

# ---------------------------------------------------------------------
log "Bringing the stack up"
docker compose up -d --wait --wait-timeout 420

log "Resetting DAG state"
airflow_cmd dags delete flight_price_pipeline -y >/dev/null 2>&1 || true
airflow_cmd dags reserialize >/dev/null
airflow_cmd dags unpause flight_price_pipeline >/dev/null

# ---------------------------------------------------------------------
log "Run 1: corrupted fixture, relaxed threshold — full pipeline must complete"
airflow_cmd dags trigger flight_price_pipeline \
  --run-id "$RUN_ID" \
  --conf "{\"source_csv_path\": \"$FIXTURE\", \"reject_rate_threshold\": $RELAXED_THRESHOLD}" >/dev/null
wait_for_run "$RUN_ID"

assert_eq "$(count_state "$RUN_ID" success)" 14 "tasks succeeded"

log "Asserting quarantine behaviour"
assert_eq "$(mysql_scalar 'SELECT COUNT(*) FROM raw_flight_prices')" 32 "rows landed"
assert_eq "$(mysql_scalar 'SELECT COUNT(DISTINCT raw_row_num) FROM rejects_flight_prices')" 16 "rows quarantined"
assert_eq "$(mysql_scalar 'SELECT COUNT(DISTINCT reason_code) FROM rejects_flight_prices')" 12 "distinct reject reasons"
assert_eq "$(mysql_scalar 'SELECT COUNT(*) FROM stg_flights')" 16 "clean rows promoted"
assert_eq "$(mysql_scalar 'SELECT COUNT(*) FROM ref_airports')" 20 "reference airports seeded"
assert_eq "$(mysql_scalar 'SELECT COUNT(*) FROM ref_allowed_values')" 10 "allowed values seeded"

log "Asserting analytics load and reconciliation"
assert_eq "$(pg_scalar 'SELECT COUNT(*) FROM fct_flights')" 16 "fact rows"
assert_eq "$(pg_scalar 'SELECT SUM(bookings) FROM kpi_bookings_by_airline')" 16 "KPI bookings reconcile"
assert_eq "$(pg_scalar 'SELECT COUNT(*) FROM fct_flights WHERE total_fare_computed_bdt <> base_fare_bdt + tax_surcharge_bdt')" 0 "fare arithmetic internally consistent"
# Each mart is built as kpi_x__new and renamed into place. A leftover __new
# table means a swap did not complete, which would otherwise go unnoticed
# because the live mart still holds the previous run's data.
leftover_swaps="$(pg_scalar "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE '%__new'")"
assert_eq "$leftover_swaps" 0 "no half-finished mart swaps left behind"

# ---------------------------------------------------------------------
log "Run 2: same fixture at the DEFAULT threshold — the gate must BLOCK it"
airflow_cmd dags delete flight_price_pipeline -y >/dev/null 2>&1 || true
airflow_cmd dags reserialize >/dev/null
airflow_cmd dags unpause flight_price_pipeline >/dev/null

STRICT_RUN="${RUN_ID}_strict"
airflow_cmd dags trigger flight_price_pipeline \
  --run-id "$STRICT_RUN" \
  --conf "{\"source_csv_path\": \"$FIXTURE\"}" >/dev/null

# The gate fails and retries, so wait for it to leave 'running' rather than
# for the whole run to settle.
waited=0
while [ "$waited" -lt 120 ]; do
  gate="$(airflow_cmd tasks states-for-dag-run flight_price_pipeline "$STRICT_RUN" -o plain 2>/dev/null \
          | grep assert_reject_rate | awk '{print $3}' || true)"
  case "$gate" in up_for_retry|failed) break ;; esac
  sleep 5
  waited=$((waited + 5))
done

[ "$gate" = "up_for_retry" ] || [ "$gate" = "failed" ] \
  || fail "gate should have blocked a 48% reject rate, state was '${gate:-<none>}'"
printf '    ok  %-42s %s\n' "gate blocked the bad batch" "$gate"

assert_eq "$(count_state "$STRICT_RUN" success)" 5 "tasks before the gate ran"
assert_eq "$(pg_scalar 'SELECT COUNT(*) FROM fct_flights')" 16 "analytics untouched by blocked run"

log "Integration test passed"
