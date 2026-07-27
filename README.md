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
- **Superuser:** `lakehouse_pg_user` · **Password:** `lakehouse_pg_password`
- **Databases:** `airflow` (Airflow metadata) and `metadata` (the platform's pipeline registry)

All of this is configured once in `pixi.toml` under `[activation.env]` (`PGDATA`, `PGHOST`,
`PGPORT`, `PGUSER`, `PGPASSWORD`), so every `pixi run` command — psql, Airflow, the tasks
below — picks it up automatically.

### How `lakehouse_pg_user` gets created

`pixi run db-init` runs `scripts/init_db.sh`, which converges the cluster to the desired
state and is safe to re-run any time:

1. If `.pgdata/` doesn't exist: `initdb -D "$PGDATA" -U lakehouse_pg_user -A scram-sha-256
   --pwfile=<tmpfile>` — this creates the cluster with `lakehouse_pg_user` as its
   **bootstrap superuser**, with password authentication enforced. No separate
   `CREATE ROLE` is needed.
2. Starts the server if it isn't running.
3. Creates the `airflow` and `metadata` databases if missing (owned by `lakehouse_pg_user`).
4. Applies `scripts/metadata_schema.sql` to `metadata` (idempotent DDL + seed data).

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

## Metadata-driven pipelines

The `metadata` database drives which DAGs exist and when they run. One row in
`data_pipelines` = one DAG in Airflow:

| Column | Type | Meaning |
| --- | --- | --- |
| `pipeline_name` | text, PK | Becomes DAG id `pipeline_<name>` |
| `schedule` | text | CRON expression the DAG runs on |
| `is_schedule_paused` | boolean | `true` pauses *scheduled* runs (manual triggers still work) |

Two pieces make this work:

- **DAG factory** (`orchestration/dags/metadata_pipelines.py`): at parse time it reads
  `data_pipelines` and builds one DAG per row, with the row's cron as the schedule.
- **`PausableCronTimetable`** (`orchestration/plugins/pausable_cron_timetable.py`, an Airflow
  plugin — timetable code runs inside the Scheduler/Dag Processor, so it must live in the
  plugins folder): a `CronTriggerTimetable` that re-checks `is_schedule_paused` in the DB every
  time the next run is computed. The Dag Processor recomputes this on every reparse
  (~30s cadence), so flipping the flag takes effect within about a reparse cycle. If the
  metadata DB is unreachable it fails open (schedules normally) and logs a warning.

Add a pipeline:

```sql
-- pixi run psql -d metadata
INSERT INTO data_pipelines (pipeline_name, schedule) VALUES ('my_pipeline', '0 6 * * *');
```

The DAG `pipeline_my_pipeline` appears after the next reparse. Pause / resume scheduling:

```sql
UPDATE data_pipelines SET is_schedule_paused = true  WHERE pipeline_name = 'my_pipeline';
UPDATE data_pipelines SET is_schedule_paused = false WHERE pipeline_name = 'my_pipeline';
```

Caveats (by design, see the module docstrings):

- **Bounded race:** the scheduler consumes an already-stored next-run date without re-calling
  the timetable, so pausing just before a due date can let one final run through.
- **Manual triggers are never blocked** by this flag — unlike Airflow's native DAG pause,
  which freezes manual runs too. Use whichever matches your intent.

## Commands

| Command | What it does |
| --- | --- |
| `pixi run start` | Start Airflow standalone (starts Postgres first if it's down) |
| `pixi run db-init` | One-time cluster + user + database creation |
| `pixi run db-start` / `db-stop` | Start / stop the bundled Postgres |
| `pixi run psql -d airflow` / `-d metadata` | psql shell on either database |
| `pixi run airflow ...` | Any Airflow CLI command (e.g. `pixi run airflow dags list`) |
| `pixi run lint` / `format` | ruff check / format |
| `pixi run typecheck` | ty on the DAGs |

## Layout

Each module follows a `src` / `tests` layout; `orchestration` is the first module and the
template for future ones (e.g. `ingestion`):

- `orchestration/src/dags/` — Airflow DAGs (incl. the metadata-driven DAG factory)
- `orchestration/src/plugins/` — Airflow plugins (custom timetable)
- `orchestration/tests/unit/` — fast tests, no database
- `orchestration/tests/integration/` — tests against a throwaway Postgres cluster
- `orchestration/pytest.ini` — pytest config for this module
- `orchestration/airflow_home/` — Airflow runtime home (gitignored: logs, generated config)
- `scripts/init_db.sh` — cluster bootstrap/converge (see above)
- `scripts/metadata_schema.sql` — metadata DB schema + seed
- `.pgdata/` — Postgres data directory (gitignored)

## Testing

`pixi run test` runs everything; scope with `pixi run pytest orchestration/tests/unit`
(or `.../integration`).

- **Unit tests** monkeypatch the pause-flag lookup and cover the timetable's scheduling,
  serialization, and fail-open behaviour with no database involved.
- **Integration tests** spin up an **ephemeral Postgres cluster** in a temp dir on a random
  port (session fixture in `orchestration/tests/integration/conftest.py`, reusing
  `scripts/init_db.sh` so the bootstrap script is itself under test), migrate the Airflow
  schema, then exercise the real DAG factory and run a pipeline end-to-end with `dag.test()`.
  Your dev cluster in `.pgdata/` is never touched.
