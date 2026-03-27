from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Tuple

from ops_sentinel.contracts.event_contract import Event, validate_event


@dataclass(frozen=True)
class BusRecord:
    topic: str
    offset: int
    event: Event


class LocalKafkaBus:
    """A local Kafka-like append-only event log with consumer group offsets."""

    def __init__(self) -> None:
        self._topics: Dict[str, List[Event]] = defaultdict(list)
        self._conds: Dict[str, asyncio.Condition] = defaultdict(asyncio.Condition)
        self._group_offsets: Dict[Tuple[str, str], int] = defaultdict(int)
        self._lock = asyncio.Lock()

    async def publish(self, topic: str, event: Event) -> int:
        validate_event(event)
        async with self._lock:
            topic_events = self._topics[topic]
            topic_events.append(event)
            offset = len(topic_events) - 1
            cond = self._conds[topic]
        async with cond:
            cond.notify_all()
        return offset

    async def consume(self, topic: str, group: str) -> BusRecord:
        cond = self._conds[topic]
        while True:
            async with self._lock:
                offset = self._group_offsets[(topic, group)]
                events = self._topics[topic]
                if offset < len(events):
                    event = events[offset]
                    self._group_offsets[(topic, group)] = offset + 1
                    return BusRecord(topic=topic, offset=offset, event=event)
            async with cond:
                await cond.wait()

    async def get_topic_size(self, topic: str) -> int:
        async with self._lock:
            return len(self._topics[topic])

    async def snapshot_offsets(self) -> Dict[str, int]:
        async with self._lock:
            return {
                f"{topic}:{group}": offset
                for (topic, group), offset in self._group_offsets.items()
            }
