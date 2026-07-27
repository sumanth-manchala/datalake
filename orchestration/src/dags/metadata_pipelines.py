"""Dynamic DAG factory: one DAG per row in metadata.data_pipelines.

Runs at parse time inside the Dag Processor's subprocess. Each pipeline row yields a
DAG whose cron schedule comes from the DB, wrapped in PausableCronTimetable so the
is_schedule_paused flag pauses scheduled runs (manual triggers keep working).
"""

from __future__ import annotations

import logging
import os

import psycopg2
from airflow.sdk import dag, task
from pausable_cron_timetable import PausableCronTimetable

log = logging.getLogger(__name__)


def _fetch_pipelines() -> list[tuple[str, str]]:
    try:
        with psycopg2.connect(
            host=os.environ["PGHOST"],
            port=os.environ["PGPORT"],
            user=os.environ["PGUSER"],
            password=os.environ["PGPASSWORD"],
            dbname="metadata",
            connect_timeout=5,
        ) as conn, conn.cursor() as cur:
            cur.execute("SELECT pipeline_name, schedule FROM data_pipelines")
            return cur.fetchall()
    except Exception:
        log.exception("Could not read data_pipelines from metadata DB; no dynamic DAGs created")
        return []


def _build_dag(pipeline_name: str, schedule: str):
    @dag(
        dag_id=f"pipeline_{pipeline_name}",
        schedule=PausableCronTimetable(schedule, pipeline_name=pipeline_name),
        catchup=False,
        tags=["metadata-driven"],
    )
    def pipeline():
        @task
        def run_pipeline() -> None:
            print(f"Running metadata-driven pipeline: {pipeline_name}")

        run_pipeline()

    return pipeline()


for name, cron in _fetch_pipelines():
    # expose each DAG as a module attribute so tests (and DagBag) can reach it
    globals()[f"pipeline_{name}"] = _build_dag(name, cron)
