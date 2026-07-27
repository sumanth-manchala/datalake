#!/usr/bin/env bash
# Converge the repo-local Postgres cluster to the desired state. Safe to re-run.
# - Creates the cluster with lakehouse_pg_user as superuser (initdb) on first run
# - Ensures the server is running
# - Ensures the 'airflow' and 'metadata' databases exist
# - Applies scripts/metadata_schema.sql (idempotent DDL + seed)
# All connection settings (PGDATA, PGPORT, PGUSER, PGPASSWORD) come from pixi's [activation.env].
set -euo pipefail
cd "$(dirname "$0")/.."

if [ ! -d "$PGDATA" ]; then
  PWFILE=$(mktemp)
  trap 'rm -f "$PWFILE"' EXIT
  printf '%s' "$PGPASSWORD" > "$PWFILE"
  initdb -D "$PGDATA" -U "$PGUSER" -A scram-sha-256 --pwfile="$PWFILE"
fi

pg_ctl status > /dev/null 2>&1 || pg_ctl -l "$PGDATA/postgres.log" start

for db in airflow metadata; do
  if ! psql -lqt | cut -d '|' -f 1 | grep -qw "$db"; then
    createdb "$db"
    echo "Created database '$db'."
  fi
done

psql -q -d metadata -v ON_ERROR_STOP=1 -f scripts/metadata_schema.sql

echo "Done: cluster at $PGDATA (port $PGPORT), superuser '$PGUSER', databases: airflow, metadata."
