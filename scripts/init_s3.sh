#!/usr/bin/env bash
# Converge the repo-local object stores to the desired state. Safe to re-run.
# - Starts SeaweedFS (S3 on :8333) and RustFS (S3 on :9010) if not already running
# - Ensures the 'lakehouse' bucket exists on both
# Data lives in .s3data/ (gitignored); pid files there let s3-stop find the processes.
set -euo pipefail
cd "$(dirname "$0")/.."

S3DATA="$PWD/.s3data"
mkdir -p "$S3DATA/seaweedfs" "$S3DATA/rustfs"

start_if_down() { # name pidfile logfile cmd...
  local name=$1 pidfile=$2 logfile=$3
  shift 3
  if [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2> /dev/null; then
    echo "$name already running (pid $(cat "$pidfile"))."
  else
    nohup "$@" > "$logfile" 2>&1 &
    echo $! > "$pidfile"
    echo "Started $name (pid $!)."
  fi
}

# SeaweedFS: every port pinned — its volume server would otherwise take 8080 (Airflow's).
start_if_down seaweedfs "$S3DATA/seaweedfs.pid" "$S3DATA/seaweedfs.log" \
  weed server -dir="$S3DATA/seaweedfs" -ip=localhost \
  -master.port=9333 -volume.port=9334 -filer -filer.port=9335 -s3 -s3.port=8333

start_if_down rustfs "$S3DATA/rustfs.pid" "$S3DATA/rustfs.log" \
  rustfs server --address 127.0.0.1:9010 \
  --access-key lakehouse --secret-key lakehouse_secret "$S3DATA/rustfs"

python - << 'EOF'
import time

import s3fs

for name, endpoint in [("seaweedfs", "http://localhost:8333"), ("rustfs", "http://localhost:9010")]:
    fs = s3fs.S3FileSystem(endpoint_url=endpoint, key="lakehouse", secret="lakehouse_secret")
    for attempt in range(30):
        try:
            fs.ls("")
            break
        except Exception:  # botocore raises its own ConnectionError while the store boots
            time.sleep(1)
    else:
        raise SystemExit(f"{name}: S3 endpoint {endpoint} did not come up")
    if not fs.exists("lakehouse"):
        fs.mkdir("lakehouse")
        print(f"{name}: created bucket 'lakehouse'.")
    else:
        print(f"{name}: bucket 'lakehouse' exists.")
EOF

echo "Done: SeaweedFS S3 on :8333, RustFS S3 on :9010, data in .s3data/."
