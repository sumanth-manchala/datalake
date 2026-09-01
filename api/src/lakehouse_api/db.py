"""Metadata-DB access for the control plane."""

import os

import psycopg2
from psycopg2.extras import RealDictCursor


def connect():
    # Connection settings come from the PG* env vars (pixi [activation.env]).
    # autocommit + no `with conn:` blocks — see AGENTS/memory: a with-block would
    # hold writes invisible until exit even with autocommit on.
    conn = psycopg2.connect(
        dbname=os.environ.get("LAKEHOUSE_METADATA_DB", "metadata"),
        cursor_factory=RealDictCursor,
    )
    conn.autocommit = True
    return conn


def get_db():
    conn = connect()
    try:
        yield conn
    finally:
        conn.close()
