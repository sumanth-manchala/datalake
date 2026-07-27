"""Unit tests for PausableCronTimetable — no database, pause flag is monkeypatched."""

from datetime import UTC, datetime

import pausable_cron_timetable as pct
import pendulum
import pytest
from airflow.timetables.base import DataInterval, TimeRestriction
from pausable_cron_timetable import PausableCronTimetable


@pytest.fixture
def timetable() -> PausableCronTimetable:
    return PausableCronTimetable("*/5 * * * *", pipeline_name="sample_pipeline")


@pytest.fixture
def restriction() -> TimeRestriction:
    return TimeRestriction(earliest=None, latest=None, catchup=False)


def last_interval() -> DataInterval:
    start = pendulum.datetime(2026, 1, 1, 12, 0, tz="UTC")
    return DataInterval(start=start, end=start)


def test_paused_returns_no_runs(timetable, restriction, monkeypatch):
    monkeypatch.setattr(pct, "_is_paused", lambda name: True)
    assert (
        timetable.next_dagrun_info(
            last_automated_data_interval=last_interval(), restriction=restriction
        )
        is None
    )


def test_unpaused_delegates_to_cron(timetable, monkeypatch):
    monkeypatch.setattr(pct, "_is_paused", lambda name: False)
    # catchup=True makes the next slot deterministic: 5 min after the last interval
    info = timetable.next_dagrun_info(
        last_automated_data_interval=last_interval(),
        restriction=TimeRestriction(earliest=None, latest=None, catchup=True),
    )
    assert info is not None
    assert info.run_after == pendulum.datetime(2026, 1, 1, 12, 5, tz="UTC")


def test_pause_check_uses_pipeline_name(timetable, restriction, monkeypatch):
    seen = []
    monkeypatch.setattr(pct, "_is_paused", lambda name: seen.append(name) or False)
    timetable.next_dagrun_info(
        last_automated_data_interval=last_interval(), restriction=restriction
    )
    assert seen == ["sample_pipeline"]


def test_serialize_round_trip(timetable):
    data = timetable.serialize()
    assert data["pipeline_name"] == "sample_pipeline"
    restored = PausableCronTimetable.deserialize(data)
    assert isinstance(restored, PausableCronTimetable)
    assert restored.pipeline_name == "sample_pipeline"
    assert restored.serialize() == data


def test_summary_names_pipeline(timetable):
    assert timetable.summary == "*/5 * * * * (pausable: sample_pipeline)"


def test_is_paused_fails_open_on_db_error(monkeypatch):
    def boom(**kwargs):
        raise ConnectionError("db down")

    monkeypatch.setattr(pct.psycopg2, "connect", boom)
    assert pct._is_paused("sample_pipeline") is False


def test_manual_run_data_interval_ignores_pause(timetable, monkeypatch):
    # manual triggers never consult the pause flag
    monkeypatch.setattr(
        pct, "_is_paused", lambda name: pytest.fail("pause flag must not be consulted")
    )
    run_after = pendulum.instance(datetime(2026, 1, 1, 12, 3, tzinfo=UTC))
    interval = timetable.infer_manual_data_interval(run_after=run_after)
    assert interval.end == run_after
