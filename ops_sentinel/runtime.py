from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Dict

from ops_sentinel.bus.event_bus import LocalKafkaBus
from ops_sentinel.services.action_service import ActionService
from ops_sentinel.services.agent_service import AgentService
from ops_sentinel.services.event_processor_service import EventProcessorService
from ops_sentinel.services.event_producer import EventProducer
from ops_sentinel.services.exception_engine import ExceptionEngine
from ops_sentinel.state_store.state_store import StateStore
from ops_sentinel.topology import (
    ACTIONS_DECIDED_TOPIC,
    ACTIONS_EXECUTED_TOPIC,
    EXCEPTIONS_RESOLVED_TOPIC,
    EXCEPTIONS_TOPIC,
    PROCESSED_TOPIC,
    RAW_INGEST_TOPIC,
)


@dataclass
class RuntimeSummary:
    topic_sizes: Dict[str, int]
    offsets: Dict[str, int]
    state: Dict[str, Any]


class OpsSentinelRuntime:
    def __init__(self, max_events: int = 40, produce_interval_s: float = 0.15) -> None:
        self.bus = LocalKafkaBus()
        self.state_store = StateStore()
        self.services = [
            EventProducer(
                bus=self.bus,
                topic_out=RAW_INGEST_TOPIC,
                max_events=max_events,
                interval_s=produce_interval_s,
            ),
            EventProcessorService(
                bus=self.bus,
                state_store=self.state_store,
                topic_in=RAW_INGEST_TOPIC,
                topic_out=PROCESSED_TOPIC,
            ),
            ExceptionEngine(
                bus=self.bus,
                state_store=self.state_store,
                topic_in=PROCESSED_TOPIC,
                topic_out=EXCEPTIONS_TOPIC,
            ),
            AgentService(
                bus=self.bus,
                state_store=self.state_store,
                topic_in=EXCEPTIONS_TOPIC,
                topic_out=ACTIONS_DECIDED_TOPIC,
            ),
            ActionService(
                bus=self.bus,
                state_store=self.state_store,
                topic_in=ACTIONS_DECIDED_TOPIC,
                topic_out_executed=ACTIONS_EXECUTED_TOPIC,
                topic_out_resolved=EXCEPTIONS_RESOLVED_TOPIC,
            ),
        ]

    async def run(self, grace_period_s: float = 1.0) -> RuntimeSummary:
        tasks = [asyncio.create_task(service.run(), name=service.name) for service in self.services]

        # Producer is finite; processing services are unbounded.
        await tasks[0]
        await asyncio.sleep(grace_period_s)

        for service in self.services[1:]:
            await service.stop()

        for task in tasks[1:]:
            task.cancel()

        await asyncio.gather(*tasks[1:], return_exceptions=True)

        topics = [
            RAW_INGEST_TOPIC,
            PROCESSED_TOPIC,
            EXCEPTIONS_TOPIC,
            ACTIONS_DECIDED_TOPIC,
            ACTIONS_EXECUTED_TOPIC,
            EXCEPTIONS_RESOLVED_TOPIC,
        ]
        topic_sizes = {topic: await self.bus.get_topic_size(topic) for topic in topics}

        return RuntimeSummary(
            topic_sizes=topic_sizes,
            offsets=await self.bus.snapshot_offsets(),
            state=await self.state_store.snapshot(),
        )
