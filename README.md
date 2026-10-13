# datalake

Self-contained local data platform: Apache Airflow orchestration and an Iceberg-based
storage layer, backed by a repo-local PostgreSQL cluster and two bundled S3-compatible
object stores. Every dependency — Python, Airflow, Postgres, the object stores — is
installed from conda-forge by [Pixi](https://pixi.sh), so the only thing you need on your
machine is Pixi.

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
| `pyiceberg` | ≥0.11 | Iceberg tables (catalog + direct data I/O) |
| `pyarrow` / `pandas` / `python-duckdb` | latest | Read/write engines for the SDK |
| `fastapi` / `uvicorn` | latest | The lakehouse control-plane API |
| `httpx` | ≥0.28 | SDK ↔ API client |
| `s3fs` | ≥2026.6 | S3 filesystem for pyiceberg + bucket bootstrap |
| `seaweedfs` | ≥4.40 | Bundled object store #1 (S3 gateway on :8333) |
| `rustfs` | ≥1.0.0b | Bundled object store #2 (S3 on :9010) |
| `ruff` | latest | Linter + formatter |
| `ty` | latest | Type checker |

Exact resolved versions are pinned in `pixi.lock`.

## Quickstart

```sh
pixi install                  # 1. create the environment
pixi run db-init              # 2. one-time: create the local Postgres cluster + airflow DB
pixi run airflow db migrate   # 3. one-time: create Airflow's tables
pixi run start                # 4. start Airflow (also starts Postgres if needed)
pixi run s3-start             # 5. start the object stores (storage layer)
pixi run api-start            # 6. start the lakehouse control-plane API on :8000
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

## Event-driven transfer queue

Adhoc/bulk data-transfer requests are rows in `metadata.transfer_requests`
(`channel`, `priority`, `payload`; lifecycle `pending → dispatched → running → done|failed`).
Instead of a polling sensor, Airflow 3 event-driven scheduling picks them up:

- **Poll trigger** (`orchestration/plugins/transfer_triggers.py`): a `BaseEventTrigger` per
  channel runs in the Triggerer and emits one asset event per new pending row (~5s latency;
  plain ANSI SQL, so any RDBMS works by swapping the driver).
- **Dispatcher** (`orchestration/dags/transfer_dags.py`): one DAG scheduled on *any* lane's
  asset **or** a minutely cron sweep. It routes each event to its lane's worker DAG,
  priority-sorted, and fans out one worker run per request via `TriggerDagRunOperator`.
  The guarded `pending → dispatched` transition dedupes duplicate events; the cron sweep
  re-dispatches requests whose wake-up event Airflow lost (a real race, observed live —
  an event landing mid-scheduler-evaluation can be absorbed without creating a run).
- **Workers** `transfer_adhoc` / `transfer_bulk`: one DAG run per request
  (`conf["ticket"]`), tasks `start → stage → transform → load`, `max_active_runs=32` per
  lane — bulk loads can never starve adhoc requests. Clearing a failed run retries the
  *same* request.

Submit a request (the rest is automatic):

```sql
-- pixi run psql -d metadata
INSERT INTO transfer_requests (channel, priority, payload)
VALUES ('adhoc', 5, '{"src": "s3://bucket/path"}');
```

## Storage layer: Datasets on Iceberg

Structured data lives in **Datasets** (Iceberg tables today — the SDK hides the format, so
Hudi later is a config change, not a user-code change). File data will land as **Filesets**
in a later phase. The moving parts:

- **Control-plane API** (`api/`, package `lakehouse_api`): FastAPI on :8000. Owns the
  registry, resolution docs, and the lineage ledger. Data bytes never flow through it.
- **SDK** (`sdk/`, package `lakehouse`): what users import. Resolves a dataset via the API,
  then reads/writes storage directly with pyiceberg.
- **Warehouses**: a catalog+storage pairing registered in the `warehouses` table. Local dev
  seeds two — `local-seaweedfs` and `local-rustfs` — both using a pyiceberg SQL catalog in
  the `metadata` DB. Vendor pairings (AWS Glue+S3, GCS+BigLake…) are future rows with a
  different `platform`; the SDK picks the placement matching `LAKEHOUSE_PLATFORM`
  (default `local`). With two local warehouses, pass `warehouse_name` when creating.

### SDK quickstart

```python
import pyarrow as pa
from lakehouse import connect

lake = connect("http://localhost:8000")
ds = lake.create_dataset(
    "sales.orders",
    schema={"day": "string", "amount": "long"},
    partition_by=["day"],  # required, non-empty
    warehouse_name="local-seaweedfs",
)
# complex types: Iceberg schema JSON (no field ids), or just pass a pyarrow schema
lake.create_dataset(
    "crm.events",
    schema={
        "day": "date",
        "amount": "decimal(18,2)",
        "tags": {"type": "list", "element": "string"},
        "attrs": {"type": "map", "key": "string", "value": "long"},
        "customer": {"type": "struct", "fields": [{"name": "id", "type": "long"}]},
    },
    partition_by=["day"],  # partition columns must be primitive
    warehouse_name="local-seaweedfs",
)
# the only write: dynamic partition overwrite — replaces only the partitions present
# in the frame, so reruns are idempotent. pandas or arrow; empty frames are rejected.
op = ds.overwrite(pa.table({"day": ["2026-08-01"], "amount": [2]}))
ds.to_pandas(where="amount > 0")  # also to_arrow() / to_duckdb("orders")
ds.to_pandas(as_of=op)  # time travel: op id, ISO timestamp, or snapshot id
# schema evolution is additive only: new columns are optional; anything else = new_version
ds.alter(add_columns={"channel": "string"})
ds.history()  # every write AND read: actor, process, snapshot
ds.partitions()
ds.schema(as_of=op)
ds.rollback(to=op)  # snapshot rollback, ledgered as an op
```

### Lineage, versions, rollback

- Every operation (create/overwrite/alter/read/rollback/drop) is a row in the `ops`
  ledger with `actor` + a generic process context — Airflow task context is auto-detected,
  model runs pass `connect(process=Process(kind="model_run", name=..., run_id=...))`.
- Every written row carries a hidden `_op_id` column (near-zero Parquet cost) — row-level
  provenance that survives future compaction. `to_pandas(include_lineage=True)` exposes it.
- Overwrites are safe because Iceberg is copy-on-write: old snapshots remain readable
  (`as_of=`) and restorable (`rollback`) until a future snapshot-expiry job defines the
  retention horizon.
- A backwards-**incompatible** schema goes through `create_dataset(..., new_version=True)`:
  a new version of the same logical name (physical table `_v2`), latest resolves by
  default, `lake.dataset(name, version=1)` pins the old one.
- Datasets and (future) Filesets share one `entities` registry; drops are soft, so lineage
  history outlives the data.

The API surface mirrors this: `POST/GET /v1/datasets`, `POST .../ops`, `.../alter`,
`.../rollback`, `GET .../history|schema|partitions` (all `as_of`-aware), `GET /v1/ops/{id}`,
`GET /v1/warehouses` — interactive docs at `http://localhost:8000/docs`.

## Commands

| Command | What it does |
| --- | --- |
| `pixi run start` | Start Airflow standalone (starts Postgres first if it's down) |
| `pixi run db-init` | One-time cluster + user + database creation |
| `pixi run db-start` / `db-stop` | Start / stop the bundled Postgres |
| `pixi run s3-start` / `s3-stop` | Start / stop SeaweedFS (:8333) and RustFS (:9010), create buckets |
| `pixi run api-start` | Start the lakehouse control-plane API on :8000 |
| `pixi run psql -d airflow` / `-d metadata` | psql shell on either database |
| `pixi run airflow ...` | Any Airflow CLI command (e.g. `pixi run airflow dags list`) |
| `pixi run lint` / `format` | ruff check / format |
| `pixi run typecheck` | ty on the DAGs |

## Layout

Each module follows a `src` / `tests` layout; `orchestration` is the first module and the
template for future ones (e.g. `ingestion`):

- `orchestration/src/dags/` — Airflow DAGs (incl. the metadata-driven DAG factory)
- `orchestration/src/plugins/` — Airflow plugins (custom timetable)
- `api/src/lakehouse_api/` — control-plane FastAPI app (registry, resolution, ledger)
- `sdk/src/lakehouse/` — client SDK (Dataset facade, pyiceberg handler, process context)
- `<module>/tests/unit/` — fast tests, no services (SDK handler tests use a sqlite
  catalog + local-filesystem warehouse)
- `<module>/tests/integration/` — throwaway Postgres cluster; the sdk suite also boots
  ephemeral SeaweedFS + RustFS and runs the full API+SDK flow against both
- `orchestration/airflow_home/` — Airflow runtime home (gitignored: logs, generated config)
- `scripts/init_db.sh` — cluster bootstrap/converge (see above)
- `scripts/init_s3.sh` — object-store bootstrap/converge (start + buckets, idempotent)
- `scripts/metadata_schema.sql` — metadata DB schema + seed (pipelines, warehouses,
  entities, placements, ops ledger)
- `.pgdata/`, `.s3data/` — Postgres / object-store data directories (gitignored)

## Testing

`pixi run test` runs everything (orchestration, api, sdk); scope with
`pixi run pytest <module>/tests/unit` (or `.../integration`).

- **Unit tests** monkeypatch the pause-flag lookup and cover the timetable's scheduling,
  serialization, and fail-open behaviour with no database involved.
- **Integration tests** spin up an **ephemeral Postgres cluster** in a temp dir on a random
  port (session fixture in `orchestration/tests/integration/conftest.py`, reusing
  `scripts/init_db.sh` so the bootstrap script is itself under test), migrate the Airflow
  schema, then exercise the real DAG factory and run a pipeline end-to-end with `dag.test()`.
  Your dev cluster in `.pgdata/` is never touched.
