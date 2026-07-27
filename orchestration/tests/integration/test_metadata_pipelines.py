"""Integration tests: real ephemeral Postgres, real DAG factory, real timetable."""

import importlib

import pendulum
from airflow.timetables.base import DataInterval, TimeRestriction
from airflow.utils.state import DagRunState


def _reload_factory():
    import metadata_pipelines

    return importlib.reload(metadata_pipelines)


def test_factory_creates_one_dag_per_row(metadata_conn):
    with metadata_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO data_pipelines (pipeline_name, schedule) "
            "VALUES ('it_pipeline', '0 6 * * *') ON CONFLICT (pipeline_name) DO NOTHING"
        )
    module = _reload_factory()

    seeded = module.pipeline_sample_pipeline
    added = module.pipeline_it_pipeline
    assert seeded.dag_id == "pipeline_sample_pipeline"
    assert seeded.timetable.summary == "*/5 * * * * (pausable: sample_pipeline)"
    assert added.timetable.summary == "0 6 * * * (pausable: it_pipeline)"
    assert "metadata-driven" in seeded.tags


def test_dag_runs_end_to_end(metadata_conn):
    module = _reload_factory()
    dag_run = module.pipeline_sample_pipeline.test()
    assert dag_run.state == DagRunState.SUCCESS


def test_pause_flag_read_from_real_db(metadata_conn):
    module = _reload_factory()
    timetable = module.pipeline_sample_pipeline.timetable
    restriction = TimeRestriction(earliest=None, latest=None, catchup=False)
    start = pendulum.datetime(2026, 1, 1, 12, 0, tz="UTC")
    interval = DataInterval(start=start, end=start)

    def next_info():
        return timetable.next_dagrun_info(
            last_automated_data_interval=interval, restriction=restriction
        )

    def set_paused(value: bool) -> None:
        with metadata_conn.cursor() as cur:
            cur.execute(
                "UPDATE data_pipelines SET is_schedule_paused = %s "
                "WHERE pipeline_name = 'sample_pipeline'",
                (value,),
            )

    try:
        set_paused(True)
        assert next_info() is None
        set_paused(False)
        assert next_info() is not None
    finally:
        set_paused(False)
