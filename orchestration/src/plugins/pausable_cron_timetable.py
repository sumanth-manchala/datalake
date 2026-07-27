"""Cron timetable that honours a per-pipeline pause flag in the metadata database.

Runs inside the Scheduler and Dag Processor (the sanctioned place for plugin code).
The pause flag is re-checked on every ``next_dagrun_info`` recomputation, which the
Dag Processor triggers unconditionally on each reparse — so pausing/unpausing in the
DB takes effect within one reparse cycle without any DAG file change.

Known bounded race: the scheduler consumes the already-stored next-run date without
re-calling the timetable, so a flag flipped to paused just before a due date can let
one final run through. Manual triggers never consult this timetable and always work.
"""

from __future__ import annotations

import datetime
import logging
import os
from typing import Any

import psycopg2
from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable
from airflow.timetables.trigger import CronTriggerTimetable
from dateutil.relativedelta import relativedelta

log = logging.getLogger(__name__)


def _is_paused(pipeline_name: str) -> bool:
    """Read the pause flag from metadata.data_pipelines. Fails open on any error."""
    try:
        with psycopg2.connect(
            host=os.environ["PGHOST"],
            port=os.environ["PGPORT"],
            user=os.environ["PGUSER"],
            password=os.environ["PGPASSWORD"],
            dbname="metadata",
            connect_timeout=5,
        ) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT is_schedule_paused FROM data_pipelines WHERE pipeline_name = %s",
                (pipeline_name,),
            )
            row = cur.fetchone()
            return bool(row[0]) if row else False
    except Exception:
        log.warning(
            "Could not read pause flag for pipeline %r; scheduling normally", pipeline_name
        )
        return False


class PausableCronTimetable(CronTriggerTimetable):
    """CronTriggerTimetable that returns no runs while its pipeline is paused in the DB."""

    def __init__(
        self,
        cron: str,
        *,
        pipeline_name: str,
        timezone: Any = "UTC",
        interval: datetime.timedelta | relativedelta = datetime.timedelta(),
        run_immediately: bool | datetime.timedelta = False,
    ) -> None:
        super().__init__(
            cron, timezone=timezone, interval=interval, run_immediately=run_immediately
        )
        self.pipeline_name = pipeline_name

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        if _is_paused(self.pipeline_name):
            return None
        return super().next_dagrun_info(
            last_automated_data_interval=last_automated_data_interval,
            restriction=restriction,
        )

    def serialize(self) -> dict[str, Any]:
        return {**super().serialize(), "pipeline_name": self.pipeline_name}

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Timetable:
        from airflow._shared.timezones.timezone import parse_timezone
        from airflow.serialization.decoders import decode_interval, decode_run_immediately

        return cls(
            data["expression"],
            pipeline_name=data["pipeline_name"],
            timezone=parse_timezone(data["timezone"]),
            interval=decode_interval(data["interval"]),
            run_immediately=decode_run_immediately(data.get("run_immediately", False)),
        )

    @property
    def summary(self) -> str:
        return f"{self._expression} (pausable: {self.pipeline_name})"


class PausableCronTimetablePlugin(AirflowPlugin):
    name = "pausable_cron_timetable"
    timetables = [PausableCronTimetable]
