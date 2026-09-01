"""Integration tests: real ephemeral Postgres, real poll trigger, real worker DAG."""

import asyncio

import pytest
import transfer_dags
from airflow.utils.state import DagRunState
from transfer_triggers import TransferRequestPollTrigger


@pytest.fixture
def insert_request(metadata_conn):
    with metadata_conn.cursor() as cur:  # isolate from rows left pending by other tests
        cur.execute("TRUNCATE transfer_requests")

    def _insert(channel: str = "adhoc", priority: int = 0, age: str = "0 seconds") -> int:
        with metadata_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO transfer_requests (channel, priority, payload, created_at)"
                ' VALUES (%s, %s, \'{"src": "it"}\','
                " CURRENT_TIMESTAMP - %s::interval) RETURNING request_id",
                (channel, priority, age),
            )
            return cur.fetchone()[0]

    return _insert


def _status(metadata_conn, request_id: int) -> str:
    with metadata_conn.cursor() as cur:
        cur.execute("SELECT status FROM transfer_requests WHERE request_id = %s", (request_id,))
        return cur.fetchone()[0]


def _collect_events(channel: str, n: int) -> list[dict]:
    async def collect():
        events = TransferRequestPollTrigger(channel=channel, interval=0.1).run()
        return [(await asyncio.wait_for(anext(events), timeout=10)).payload for _ in range(n)]

    return asyncio.run(collect())


def test_trigger_emits_one_event_per_row_in_its_channel_only(insert_request):
    adhoc_high = insert_request("adhoc", priority=5)
    insert_request("bulk", priority=0)
    adhoc_low = insert_request("adhoc", priority=0)

    payloads = _collect_events("adhoc", 2)

    assert payloads == [
        {"request_id": adhoc_high, "priority": 5},
        {"request_id": adhoc_low, "priority": 0},
    ]


def test_fresh_trigger_re_emits_still_pending_rows(insert_request):
    # a triggerer restart resets the high-water mark; pending rows are re-emitted once
    request_id = insert_request("bulk", priority=1)
    for _ in range(2):
        assert {"request_id": request_id, "priority": 1} in _collect_events("bulk", 1)


def test_mark_dispatched_transitions_pending_only_once(metadata_conn, insert_request):
    request_id = insert_request()

    assert transfer_dags._mark_dispatched([request_id]) == {request_id}
    assert _status(metadata_conn, request_id) == "dispatched"
    # a duplicate event's dispatch attempt fans out nothing
    assert transfer_dags._mark_dispatched([request_id]) == set()


def test_sweep_finds_only_pending_rows_past_grace(insert_request):
    orphaned = insert_request("bulk", priority=7, age="2 minutes")
    insert_request("adhoc")  # fresh: inside grace, presumed in flight
    dispatched = insert_request("bulk", age="2 minutes")
    transfer_dags._mark_dispatched([dispatched])

    assert transfer_dags._sweep_candidates() == [
        {"request_id": orphaned, "priority": 7, "dag_id": "transfer_bulk"}
    ]


def test_start_requires_dispatched_and_accepts_failed_for_retry(metadata_conn, insert_request):
    request_id = insert_request()

    assert transfer_dags._start_request(request_id) is None  # pending: no run owns it yet

    transfer_dags._mark_dispatched([request_id])
    started = transfer_dags._start_request(request_id)
    assert started == {"request_id": request_id, "payload": {"src": "it"}}
    assert _status(metadata_conn, request_id) == "running"

    assert transfer_dags._start_request(request_id) is None  # duplicate run skips

    transfer_dags._set_status(request_id, "failed")
    assert transfer_dags._start_request(request_id) is not None  # clearing a failed run retries


def test_worker_dag_processes_its_assigned_request(metadata_conn, insert_request):
    request_id = insert_request()
    transfer_dags._mark_dispatched([request_id])

    dag_run = transfer_dags.transfer_adhoc.test(run_conf={"ticket": request_id})

    assert dag_run.state == DagRunState.SUCCESS
    assert _status(metadata_conn, request_id) == "done"


def test_failure_callback_marks_running_request_failed(metadata_conn, insert_request):
    request_id = insert_request()
    transfer_dags._mark_dispatched([request_id])
    transfer_dags._start_request(request_id)

    class _Run:
        conf = {"ticket": request_id}

    transfer_dags._mark_failed({"dag_run": _Run()})
    assert _status(metadata_conn, request_id) == "failed"

    # only 'running' rows are clobbered: a done request is left alone
    transfer_dags._set_status(request_id, "done", expected="failed")
    transfer_dags._mark_failed({"dag_run": _Run()})
    assert _status(metadata_conn, request_id) == "done"
