"""Actor / platform / process resolution for lineage."""

import getpass
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Process:
    kind: str
    name: str | None = None
    run_id: str | None = None


def resolve_actor() -> str:
    return os.environ.get("LAKEHOUSE_ACTOR") or getpass.getuser()


def resolve_platform() -> str:
    return os.environ.get("LAKEHOUSE_PLATFORM", "local")


def resolve_process() -> Process:
    """Airflow task context wins, then explicit LAKEHOUSE_PROCESS_* env, else adhoc."""
    if dag_id := os.environ.get("AIRFLOW_CTX_DAG_ID"):
        run_id = os.environ.get("AIRFLOW_CTX_DAG_RUN_ID")
        task_id = os.environ.get("AIRFLOW_CTX_TASK_ID")
        return Process("pipeline", dag_id, f"{run_id}/{task_id}" if task_id else run_id)
    if kind := os.environ.get("LAKEHOUSE_PROCESS_KIND"):
        return Process(
            kind,
            os.environ.get("LAKEHOUSE_PROCESS_NAME"),
            os.environ.get("LAKEHOUSE_PROCESS_RUN_ID"),
        )
    return Process("adhoc")
