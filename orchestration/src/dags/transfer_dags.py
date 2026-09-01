"""Event-driven transfer queue: one dispatcher DAG plus a worker DAG per channel (lane).

A poll trigger per channel (see transfer_triggers.py, running in the Triggerer) emits
one asset event per new metadata.transfer_requests row. The dispatcher consumes events
from ANY lane and fans out one worker run per request, priority-sorted so higher-priority
runs are created first. Each worker run processes exactly the request in its
conf["ticket"] — 1:1 run↔request, so a request's logs are its run's logs and clearing a
failed run retries the same request. Bulk loads can never starve adhoc requests — each
lane has its own worker DAG and max_active_runs budget.

Request lifecycle: pending → dispatched (worker run created) → running → done|failed.
The dispatcher's guarded pending→dispatched transition dedupes: duplicate asset events
for a request fan out nothing.

Why the dispatcher also runs on a cron sweep (AssetOrTimeSchedule): an asset event that
lands while the scheduler is mid-evaluation can have its run-queue flag cleared by the
run being created for *earlier* events while falling outside that run's consumption
window — orphaned until the next event arrives (observed live on 3.3.0). The sweep
dispatches any request still 'pending' after a grace period, so a lost wake-up costs at
most ~a minute instead of stalling until the next insert.
"""

from __future__ import annotations

import logging
from functools import reduce
from operator import or_
from typing import Any

import psycopg2
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Asset, AssetOrTimeSchedule, AssetWatcher, CronTriggerTimetable, dag, task
from airflow.sdk.exceptions import AirflowSkipException
from transfer_triggers import TransferRequestPollTrigger

log = logging.getLogger(__name__)

CHANNELS = ("adhoc", "bulk")
WORKER_PARALLELISM = 32  # concurrent transfers per lane (was max_active_dags in the sensor design)
SWEEP_GRACE = "30 seconds"  # older pending rows are presumed orphaned and swept

ASSETS = {
    channel: Asset(
        f"metadata://transfer_requests/{channel}",
        watchers=[
            AssetWatcher(
                name=f"transfer-{channel}", trigger=TransferRequestPollTrigger(channel=channel)
            )
        ],
    )
    for channel in CHANNELS
}
ASSET_TO_DAG = {ASSETS[channel].name: f"transfer_{channel}" for channel in CHANNELS}


def _connect():
    # host/port/user/password come from the PG* env vars (libpq reads them natively)
    return psycopg2.connect(dbname="metadata", connect_timeout=5)


def _event_candidates(triggering_asset_events) -> list[dict[str, Any]]:
    """One candidate per consumed asset event, routed by the asset it arrived on."""
    return [
        {"request_id": event.extra["payload"]["request_id"],
         "priority": event.extra["payload"]["priority"],
         "dag_id": ASSET_TO_DAG[key.name]}
        for key, events in triggering_asset_events.items()
        for event in events
    ]  # fmt: skip


def _sweep_candidates() -> list[dict[str, Any]]:
    """Requests still 'pending' past the grace period — their wake-up event was lost."""
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT request_id, priority, channel FROM transfer_requests"
            " WHERE status = 'pending'"
            f" AND created_at < CURRENT_TIMESTAMP - INTERVAL '{SWEEP_GRACE}'"
        )
        rows = cur.fetchall()
    return [
        {"request_id": request_id, "priority": priority, "dag_id": f"transfer_{channel}"}
        for request_id, priority, channel in rows
    ]


def _mark_dispatched(request_ids: list[int]) -> set[int]:
    """Guarded pending→dispatched transition; drops duplicates in one statement."""
    if not request_ids:
        return set()
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE transfer_requests SET status = 'dispatched', updated_at = CURRENT_TIMESTAMP"
            " WHERE request_id IN %s AND status = 'pending'"
            " RETURNING request_id",
            (tuple(request_ids),),
        )
        return {row[0] for row in cur.fetchall()}


def _route(triggering_asset_events) -> list[dict[str, Any]]:
    """Fan-out confs: event + swept candidates, deduped, priority first.

    Known ceiling vs a claim-based worker: a later insert cannot preempt already-queued
    runs in its own lane; cross-lane priority is structural (separate lanes).
    """
    candidates = {c["request_id"]: c for c in _event_candidates(triggering_asset_events)}
    for candidate in _sweep_candidates():
        candidates.setdefault(candidate["request_id"], candidate)
    dispatched = _mark_dispatched(list(candidates))
    kept = sorted(
        (c for c in candidates.values() if c["request_id"] in dispatched),
        key=lambda c: c["priority"],
        reverse=True,
    )
    return [{"trigger_dag_id": c["dag_id"], "conf": {"ticket": c["request_id"]}} for c in kept]


def _set_status(request_id: int, status: str, expected: str = "running") -> None:
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE transfer_requests SET status = %s, updated_at = CURRENT_TIMESTAMP"
            " WHERE request_id = %s AND status = %s",
            (status, request_id, expected),
        )


def _start_request(request_id: int) -> dict[str, Any] | None:
    """Guarded start: claims this run's assigned request, or None if already handled.

    'failed' is accepted so clearing a failed run re-processes the same request.
    """
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE transfer_requests SET status = 'running', updated_at = CURRENT_TIMESTAMP"
            " WHERE request_id = %s AND status IN ('dispatched', 'failed')"
            " RETURNING payload",
            (request_id,),
        )
        row = cur.fetchone()
    return None if row is None else {"request_id": request_id, "payload": row[0]}


def _mark_failed(context) -> None:
    ticket = (context["dag_run"].conf or {}).get("ticket")
    if ticket is not None:
        _set_status(ticket, "failed")


@dag(
    dag_id="transfer_dispatcher",
    schedule=AssetOrTimeSchedule(
        # ANY-of: a list would mean ALL-of and adhoc events would wait for bulk events
        assets=reduce(or_, ASSETS.values()),
        timetable=CronTriggerTimetable("* * * * *", timezone="UTC"),  # orphan sweep
    ),
    catchup=False,
    max_active_runs=1,  # serialize dispatch: one pending→dispatched transition at a time
    tags=["transfers"],
)
def _dispatcher():
    @task
    def route(**context) -> list[dict[str, Any]]:
        return _route(context["triggering_asset_events"])

    TriggerDagRunOperator.partial(task_id="fan_out").expand_kwargs(route())


def _build_worker(channel: str):
    @dag(
        dag_id=f"transfer_{channel}",
        schedule=None,  # runs come only from the dispatcher
        catchup=False,
        max_active_runs=WORKER_PARALLELISM,
        on_failure_callback=_mark_failed,
        tags=["transfers"],
    )
    def worker():
        @task
        def start(**context) -> dict[str, Any]:
            request = _start_request(context["dag_run"].conf["ticket"])
            if request is None:
                raise AirflowSkipException("request already handled; duplicate event")
            return request

        @task
        def stage(request) -> dict[str, Any]:
            log.info("Staging transfer %s: %s", request["request_id"], request["payload"])
            return request

        @task
        def transform(request) -> dict[str, Any]:
            log.info("Transforming transfer %s", request["request_id"])
            return request

        @task
        def load(request) -> None:
            log.info("Loading transfer %s", request["request_id"])
            _set_status(request["request_id"], "done")

        load(transform(stage(start())))

    return worker()


transfer_dispatcher = _dispatcher()
transfer_adhoc = _build_worker("adhoc")
transfer_bulk = _build_worker("bulk")
