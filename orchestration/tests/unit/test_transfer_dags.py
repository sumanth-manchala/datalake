"""Unit tests for the transfer DAG module structure and event routing — no database."""

from types import SimpleNamespace

import pytest
import transfer_dags
from airflow.sdk import AssetOrTimeSchedule
from airflow.sdk.definitions.asset import Asset, AssetAny


def _event(request_id: int, priority: int) -> SimpleNamespace:
    # shape of an AssetEvent from an AssetWatcher trigger (see models/trigger.py)
    return SimpleNamespace(
        extra={"from_trigger": True, "payload": {"request_id": request_id, "priority": priority}}
    )


def test_dispatcher_schedules_on_any_lane_asset_or_sweep_tick():
    timetable = transfer_dags.transfer_dispatcher.timetable
    assert isinstance(timetable, AssetOrTimeSchedule)
    condition = timetable.asset_condition
    assert isinstance(condition, AssetAny)
    assert {asset.name for asset in condition.objects if isinstance(asset, Asset)} == {
        "metadata://transfer_requests/adhoc",
        "metadata://transfer_requests/bulk",
    }


@pytest.mark.parametrize("channel", transfer_dags.CHANNELS)
def test_worker_dags_are_trigger_only_with_capped_parallelism(channel):
    worker = getattr(transfer_dags, f"transfer_{channel}")
    assert worker.dag_id == f"transfer_{channel}"
    assert worker.schedule is None
    assert worker.max_active_runs == transfer_dags.WORKER_PARALLELISM
    assert list(worker.task_dict) == ["start", "stage", "transform", "load"]


@pytest.mark.parametrize("channel", transfer_dags.CHANNELS)
def test_each_lane_asset_has_its_own_poll_watcher(channel):
    asset = transfer_dags.ASSETS[channel]
    (watcher,) = asset.watchers
    classpath, kwargs = watcher.trigger.serialize()
    assert classpath == "transfer_triggers.TransferRequestPollTrigger"
    assert kwargs["channel"] == channel


def test_event_candidates_route_by_asset():
    events = {
        transfer_dags.ASSETS["adhoc"]: [_event(11, 5)],
        transfer_dags.ASSETS["bulk"]: [_event(12, 0), _event(13, 9)],
    }
    assert transfer_dags._event_candidates(events) == [
        {"request_id": 11, "priority": 5, "dag_id": "transfer_adhoc"},
        {"request_id": 12, "priority": 0, "dag_id": "transfer_bulk"},
        {"request_id": 13, "priority": 9, "dag_id": "transfer_bulk"},
    ]


def test_route_dedupes_merges_sweep_and_sorts_by_priority(monkeypatch):
    events = {
        transfer_dags.ASSETS["adhoc"]: [_event(11, 5)],
        transfer_dags.ASSETS["bulk"]: [_event(12, 0)],
    }
    swept = [
        {"request_id": 12, "priority": 0, "dag_id": "transfer_bulk"},  # duplicate of event
        {"request_id": 9, "priority": 7, "dag_id": "transfer_bulk"},  # orphaned wake-up
    ]
    monkeypatch.setattr(transfer_dags, "_sweep_candidates", lambda: swept)
    # request 12 lost the pending→dispatched race (e.g. dispatched by an earlier run)
    monkeypatch.setattr(transfer_dags, "_mark_dispatched", lambda ids: set(ids) - {12})

    assert transfer_dags._route(events) == [
        {"trigger_dag_id": "transfer_bulk", "conf": {"ticket": 9}},
        {"trigger_dag_id": "transfer_adhoc", "conf": {"ticket": 11}},
    ]
