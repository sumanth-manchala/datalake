"""Unit tests for TransferRequestPollTrigger — no database, fetch is monkeypatched."""

import asyncio

import transfer_triggers
from transfer_triggers import TransferRequestPollTrigger


def test_serialize_round_trips_kwargs():
    classpath, kwargs = TransferRequestPollTrigger(channel="bulk", interval=1.5).serialize()
    assert classpath == "transfer_triggers.TransferRequestPollTrigger"
    assert kwargs == {"channel": "bulk", "interval": 1.5}


def test_emits_one_event_per_row_and_advances_high_water_mark(monkeypatch):
    calls: list[tuple[str, int]] = []
    batches = [[(1, 5), (2, 0)], [], [(3, 9)]]

    def fake_fetch(channel: str, last_seen: int):
        calls.append((channel, last_seen))
        return batches.pop(0) if batches else []

    monkeypatch.setattr(transfer_triggers, "_fetch_new_requests", fake_fetch)

    async def collect(n: int):
        events = transfer_triggers.TransferRequestPollTrigger(channel="adhoc", interval=0).run()
        return [await asyncio.wait_for(anext(events), timeout=5) for _ in range(n)]

    payloads = [event.payload for event in asyncio.run(collect(3))]

    assert payloads == [
        {"request_id": 1, "priority": 5},
        {"request_id": 2, "priority": 0},
        {"request_id": 3, "priority": 9},
    ]
    # only new rows are fetched: the mark advances past emitted ids and polls the channel
    assert calls[0] == ("adhoc", 0)
    assert ("adhoc", 2) in calls


def test_poll_errors_are_swallowed_and_polling_continues(monkeypatch):
    batches = [ConnectionError("db down"), [(7, 1)]]

    def fake_fetch(channel: str, last_seen: int):
        batch = batches.pop(0)
        if isinstance(batch, Exception):
            raise batch
        return batch

    monkeypatch.setattr(transfer_triggers, "_fetch_new_requests", fake_fetch)

    async def first_event():
        events = transfer_triggers.TransferRequestPollTrigger(channel="adhoc", interval=0).run()
        return await asyncio.wait_for(anext(events), timeout=5)

    assert asyncio.run(first_event()).payload == {"request_id": 7, "priority": 1}
