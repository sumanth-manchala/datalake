# datalake

Self-contained local data platform: Apache Airflow orchestration backed by a repo-local
PostgreSQL cluster. Every dependency — Python, Airflow, Postgres itself — is installed from
conda-forge by [Pixi](https://pixi.sh), so the only thing you need on your machine is Pixi.

## Prerequisites

| Requirement | Why | Install |
| --- | --- | --- |
| [Pixi](https://pixi.sh) | Installs and pins everything else | `curl -fsSL https://pixi.sh/install.sh \| sh` |

That's it. No system Python, no system Postgres, no Docker.

## Dependencies (managed by Pixi, all from conda-forge)

| Package | Version | Purpose |
| --- | --- | --- |
| `python` | 3.12 | Runtime for Airflow and DAGs |
| `apache-airflow` | 3.3.0 | Workflow orchestrator |
| `apache-airflow-providers-postgres` | ≥6.8 | Airflow Postgres hooks/operators |
| `psycopg2` | ≥2.9 | Sync Postgres driver (Airflow metadata DB) |
| `asyncpg` | ≥0.31 | Async Postgres driver (Airflow 3 async engine) |
| `postgresql` | ≥18.4 | The bundled Postgres server + client tools |
| `ruff` | latest | Linter + formatter |
| `ty` | latest | Type checker |

Exact resolved versions are pinned in `pixi.lock`.

## Quickstart

```sh
pixi install                  # 1. create the environment
pixi run db-init              # 2. one-time: create the local Postgres cluster + airflow DB
pixi run airflow db migrate   # 3. one-time: create Airflow's tables
pixi run start                # 4. start Airflow (also starts Postgres if needed)
```

UI: http://localhost:8080 — log in as `admin` with the auto-generated password in
`orchestration/airflow_home/simple_auth_manager_passwords.json.generated`.

## The database: `lakehouse_pg_user` and the local cluster

The Postgres cluster lives entirely inside the repo:

- **Data directory:** `.pgdata/` (gitignored) · **Port:** `5433` (avoids clashing with any system Postgres on 5432)
- **Superuser:** `lakehouse_pg_user` · **Password:** `lakehouse_pg_password` · **Database:** `airflow`

All of this is configured once in `pixi.toml` under `[activation.env]` (`PGDATA`, `PGHOST`,
`PGPORT`, `PGUSER`, `PGPASSWORD`), so every `pixi run` command — psql, Airflow, the tasks
below — picks it up automatically.

### How `lakehouse_pg_user` gets created

`pixi run db-init` runs `scripts/init_db.sh`, which:

1. Skips everything if `.pgdata/` already exists (safe to re-run).
2. Runs `initdb -D "$PGDATA" -U lakehouse_pg_user -A scram-sha-256 --pwfile=<tmpfile>` —
   this creates the cluster with `lakehouse_pg_user` as its **bootstrap superuser**, with
   password authentication enforced. No separate `CREATE ROLE` is needed.
3. Starts the server (`pg_ctl start`) and runs `createdb airflow` (owned by `lakehouse_pg_user`).

Equivalent manual steps, for reference:

```sh
pixi shell                                  # env with PGDATA/PGPORT/PGUSER/PGPASSWORD set
echo "lakehouse_pg_password" > /tmp/pw
initdb -D .pgdata -U lakehouse_pg_user -A scram-sha-256 --pwfile=/tmp/pw && rm /tmp/pw
pg_ctl -l .pgdata/postgres.log start
createdb airflow
psql -d airflow -c "\conninfo"              # verify: connected as lakehouse_pg_user on 5433
```

### Changing the password

Pick a new value, then keep these two places in sync in `pixi.toml`: `PGPASSWORD` and the
password inside `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`. For an existing cluster also run
`pixi run psql -c "ALTER USER lakehouse_pg_user PASSWORD 'newvalue';"`.

## Commands

| Command | What it does |
| --- | --- |
| `pixi run start` | Start Airflow standalone (starts Postgres first if it's down) |
| `pixi run db-init` | One-time cluster + user + database creation |
| `pixi run db-start` / `db-stop` | Start / stop the bundled Postgres |
| `pixi run psql -d airflow` | Open a psql shell on the metadata DB |
| `pixi run airflow ...` | Any Airflow CLI command (e.g. `pixi run airflow dags list`) |
| `pixi run lint` / `format` | ruff check / format |
| `pixi run typecheck` | ty on the DAGs |

## Layout

- `orchestration/dags/` — Airflow DAGs
- `orchestration/airflow_home/` — Airflow runtime home (gitignored: logs, generated config)
- `scripts/init_db.sh` — cluster bootstrap (see above)
- `.pgdata/` — Postgres data directory (gitignored)
