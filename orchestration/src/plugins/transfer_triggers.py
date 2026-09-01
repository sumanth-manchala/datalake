"""Event trigger watching metadata.transfer_requests for new rows.

Runs in the Triggerer inside an AssetWatcher: one TriggerEvent per new pending row in
the watched channel, which the scheduler turns into one asset event on that channel's
Asset. Poll-based on plain ANSI SQL so any RDBMS works by swapping the DBAPI driver
(deliberately not Postgres LISTEN/NOTIFY).

It emits on state *change* (a request_id above the in-memory high-water mark), never on
state presence, so an unclaimed pending row cannot re-fire the watcher (the
infinite-rescheduling pitfall from the event-driven scheduling docs). A triggerer
restart resets the mark and re-emits pending rows once; worker runs treat duplicate
tickets as no-ops, as the docs require (duplicates are possible, lost events are not).
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import psycopg2
from airflow.triggers.base import BaseEventTrigger, TriggerEvent


def _fetch_new_requests(channel: str, last_seen: int) -> list[tuple[int, int]]:
    # host/port/user/password come from the PG* env vars (libpq reads them natively)
    with psycopg2.connect(dbname="metadata", connect_timeout=5) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT request_id, priority FROM transfer_requests"
            " WHERE status = 'pending' AND channel = %s AND request_id > %s"
            " ORDER BY request_id",
            (channel, last_seen),
        )
        return cur.fetchall()


class TransferRequestPollTrigger(BaseEventTrigger):
    """Yields one TriggerEvent({"request_id", "priority"}) per new pending row."""

    def __init__(self, channel: str, interval: float = 5.0) -> None:
        super().__init__()
        self.channel = channel
        self.interval = interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "transfer_triggers.TransferRequestPollTrigger",
            {"channel": self.channel, "interval": self.interval},
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        last_seen = 0
        while True:
            try:
                rows = await asyncio.to_thread(_fetch_new_requests, self.channel, last_seen)
            except Exception:
                self.log.exception("Could not poll transfer_requests; retrying")
                rows = []
            for request_id, priority in rows:
                yield TriggerEvent({"request_id": request_id, "priority": priority})
                last_seen = request_id
            await asyncio.sleep(self.interval)
