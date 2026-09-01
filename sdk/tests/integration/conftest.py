"""Ephemeral full stack: Postgres (metadata + iceberg catalog), SeaweedFS and RustFS S3
gateways on random ports, and the control-plane API mounted in-process via TestClient.
Reuses scripts/init_db.sh so the bootstrap script itself stays under test; the seeded
warehouse rows are repointed at the ephemeral endpoints."""

import os
import random
import shutil
import socket
import subprocess
import tempfile
import time
from pathlib import Path

import psycopg2
import pytest
import s3fs
from psycopg2.extras import Json

REPO_ROOT = Path(__file__).resolve().parents[3]
S3_KEY, S3_SECRET = "lakehouse", "lakehouse_secret"


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("localhost", 0))
        return sock.getsockname()[1]


def _free_weed_port() -> int:
    # seaweedfs opens gRPC on port+10000, so its ports must stay well below 55535 —
    # OS-assigned ephemeral ports (49152+) can exceed that and crash weed at startup
    while True:
        port = random.randint(20000, 45000)
        with socket.socket() as sock:
            try:
                sock.bind(("localhost", port))
            except OSError:
                continue
            return port


def _wait_for_s3(endpoint: str, timeout: int = 90) -> s3fs.S3FileSystem:
    fs = s3fs.S3FileSystem(
        endpoint_url=endpoint, key=S3_KEY, secret=S3_SECRET, skip_instance_cache=True
    )
    deadline = time.monotonic() + timeout
    while True:
        try:
            fs.ls("")
            return fs
        except Exception:
            if time.monotonic() > deadline:
                raise
            time.sleep(1)


@pytest.fixture(scope="session", autouse=True)
def stack():
    root = Path(tempfile.mkdtemp(prefix="lakehouse-it-"))
    pg_port = _free_port()
    os.environ.update(
        {"PGDATA": str(root / "pgdata"), "PGHOST": "localhost", "PGPORT": str(pg_port)}
    )
    subprocess.run(
        ["bash", "scripts/init_db.sh"], cwd=REPO_ROOT, check=True, capture_output=True, text=True
    )

    seaweed_s3, rust_s3 = _free_weed_port(), _free_port()
    (root / "weed").mkdir()
    (root / "rustfs").mkdir()
    log = (root / "stores.log").open("w")
    procs = [
        subprocess.Popen(
            [
                "weed",
                "server",
                f"-dir={root / 'weed'}",
                "-ip=localhost",
                f"-master.port={_free_weed_port()}",
                f"-volume.port={_free_weed_port()}",
                "-filer",
                f"-filer.port={_free_weed_port()}",
                "-s3",
                f"-s3.port={seaweed_s3}",
            ],
            stdout=log,
            stderr=log,
        ),
        subprocess.Popen(
            [
                "rustfs",
                "server",
                "--address",
                f"127.0.0.1:{rust_s3}",
                "--access-key",
                S3_KEY,
                "--secret-key",
                S3_SECRET,
                str(root / "rustfs"),
            ],
            stdout=log,
            stderr=log,
        ),
    ]
    endpoints = {"local-seaweedfs": seaweed_s3, "local-rustfs": rust_s3}
    for port in endpoints.values():
        fs = _wait_for_s3(f"http://localhost:{port}")
        if not fs.exists("lakehouse"):
            fs.mkdir("lakehouse")

    uri = (
        f"postgresql+psycopg2://{os.environ['PGUSER']}:{os.environ['PGPASSWORD']}"
        f"@localhost:{pg_port}/metadata"
    )
    conn = psycopg2.connect(dbname="metadata")
    conn.autocommit = True
    with conn.cursor() as cur:
        for warehouse_name, port in endpoints.items():
            cur.execute(
                "UPDATE warehouses SET catalog_config = %s, storage_config = %s"
                " WHERE warehouse_name = %s",
                (
                    Json({"uri": uri, "warehouse": "s3://lakehouse/warehouse"}),
                    Json(
                        {
                            "endpoint_url": f"http://localhost:{port}",
                            "bucket": "lakehouse",
                            "access_key": S3_KEY,
                            "secret_key": S3_SECRET,
                        }
                    ),
                    warehouse_name,
                ),
            )
    conn.close()

    yield

    for proc in procs:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
    log.close()
    subprocess.run(["pg_ctl", "stop", "-m", "fast"], capture_output=True)
    shutil.rmtree(root, ignore_errors=True)


@pytest.fixture
def lake(stack):
    from fastapi.testclient import TestClient
    from lakehouse import connect
    from lakehouse_api.app import app

    return connect(http=TestClient(app), actor="it-tester")
