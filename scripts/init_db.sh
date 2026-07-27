#!/usr/bin/env bash
# One-time init of the repo-local Postgres cluster.
# Creates lakehouse_pg_user as the cluster superuser (via initdb) and the airflow database.
# All connection settings (PGDATA, PGPORT, PGUSER, PGPASSWORD) come from pixi's [activation.env].
set -euo pipefail

if [ -d "$PGDATA" ]; then
  echo "Cluster already initialized at $PGDATA — nothing to do."
  exit 0
fi

PWFILE=$(mktemp)
trap 'rm -f "$PWFILE"' EXIT
printf '%s' "$PGPASSWORD" > "$PWFILE"

initdb -D "$PGDATA" -U "$PGUSER" -A scram-sha-256 --pwfile="$PWFILE"

pg_ctl -l "$PGDATA/postgres.log" start
createdb airflow

echo "Done: cluster at $PGDATA (port $PGPORT), superuser '$PGUSER', database 'airflow'."
