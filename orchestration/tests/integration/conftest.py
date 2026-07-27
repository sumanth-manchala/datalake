"""Provision the ephemeral Postgres cluster for integration tests.

Reuses scripts/init_db.sh (so the bootstrap script itself is exercised) against the
PGDATA/PGPORT set by the session-level conftest, then migrates the Airflow schema.
"""

import os
import shutil
import subprocess
from pathlib import Path

import psycopg2
import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]


def _run(*cmd: str) -> None:
    subprocess.run(cmd, check=True, capture_output=True, text=True, cwd=REPO_ROOT)


@pytest.fixture(scope="session", autouse=True)
def ephemeral_cluster():
    _run("bash", "scripts/init_db.sh")
    _run("airflow", "db", "migrate")
    yield
    # release airflow's pooled connections before the server goes away,
    # otherwise sqlalchemy prints a scary (but harmless) error at shutdown
    from airflow import settings

    settings.dispose_orm(do_log=False)
    subprocess.run(["pg_ctl", "stop", "-m", "fast"], capture_output=True)
    shutil.rmtree(os.environ["PGDATA"], ignore_errors=True)


@pytest.fixture
def metadata_conn(ephemeral_cluster):
    # No `with conn:` here — psycopg2 runs statements in a transaction inside a
    # connection context manager even when autocommit is True, so writes would
    # stay invisible to the code under test until the block exits.
    conn = psycopg2.connect(
        host=os.environ["PGHOST"],
        port=os.environ["PGPORT"],
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
        dbname="metadata",
    )
    conn.autocommit = True
    yield conn
    conn.close()
