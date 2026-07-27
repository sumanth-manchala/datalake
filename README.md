# datalake

Local data platform managed with [Pixi](https://pixi.sh).

## Layout

- `orchestration/dags/` — Airflow DAGs
- `orchestration/airflow_home/` — Airflow runtime home (gitignored: logs, generated config)

## Setup

Requires Pixi and a local Postgres on `localhost:5432` with an `airflow` database.

```sh
pixi install
pixi run airflow db migrate   # one-time metadata DB init
```

## Commands

| Command | What it does |
| --- | --- |
| `pixi run start` | Start Airflow standalone (UI at http://localhost:8080) |
| `pixi run airflow ...` | Any airflow CLI command (e.g. `pixi run airflow dags list`) |
| `pixi run lint` | ruff check |
| `pixi run format` | ruff format |
| `pixi run typecheck` | ty on the DAGs |

Admin login is auto-generated at `orchestration/airflow_home/simple_auth_manager_passwords.json.generated`.
