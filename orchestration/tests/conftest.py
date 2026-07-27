"""Point Airflow and PG* env at an ephemeral test cluster for the whole pytest session.

This runs at collection time, BEFORE any test module imports airflow — airflow reads its
config once at import, so the environment must be in place first. Unit tests never touch
the database; integration tests get the cluster actually started by their own conftest.
"""

import os
import socket
import tempfile
from pathlib import Path

with socket.socket() as _s:
    _s.bind(("localhost", 0))
    EPHEMERAL_PORT = _s.getsockname()[1]

EPHEMERAL_ROOT = Path(tempfile.mkdtemp(prefix="datalake-test-"))

_user = os.environ.get("PGUSER", "lakehouse_pg_user")
_password = os.environ.get("PGPASSWORD", "lakehouse_pg_password")

os.environ.update(
    {
        "PGDATA": str(EPHEMERAL_ROOT / "pgdata"),
        "PGHOST": "localhost",
        "PGPORT": str(EPHEMERAL_PORT),
        "PGUSER": _user,
        "PGPASSWORD": _password,
        "AIRFLOW_HOME": str(EPHEMERAL_ROOT / "airflow_home"),
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": (
            f"postgresql+psycopg2://{_user}:{_password}@localhost:{EPHEMERAL_PORT}/airflow"
        ),
    }
)
