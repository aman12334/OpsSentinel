from __future__ import annotations

import asyncio

from ops_sentinel.bus.event_bus import LocalKafkaBus
from ops_sentinel.contracts.event_contract import build_event
from ops_sentinel.services.base import BaseService
from ops_sentinel.state_store.state_store import StateStore


class ActionService(BaseService):
    """Service 7: executes actions and emits resolution updates."""

    def __init__(
        self,
        bus: LocalKafkaBus,
        state_store: StateStore,
        topic_in: str,
        topic_out_executed: str,
        topic_out_resolved: str,
        consumer_group: str = "action-service-v1",
    ) -> None:
        super().__init__(name="action-service")
        self.bus = bus
        self.state_store = state_store
        self.topic_in = topic_in
        self.topic_out_executed = topic_out_executed
        self.topic_out_resolved = topic_out_resolved
        self.consumer_group = consumer_group

    async def run(self) -> None:
        while self.running:
            record = await self.bus.consume(self.topic_in, self.consumer_group)
            event = record.event
            action_id = event["signals"].get("action_id", "unknown")
            action_name = event["signals"].get("action_name", "monitor")
            exception_id = event["signals"].get("exception_id")

            # Simulate asynchronous side effects and retries in real systems.
            await asyncio.sleep(0.05)

            await self.state_store.record_action(
                action_id,
                {
                    "status": "executed",
                    "entity": event["entity"],
                    "action_name": action_name,
                    "executed_from_event": event["event_id"],
                },
            )

            executed_event = build_event(
                event_type="action.executed",
                source=self.name,
                entity_type=event["entity"]["type"],
                entity_id=event["entity"]["id"],
                severity=event["severity"],
                text=f"Action executed: {action_name}",
                signals={
                    "action_id": action_id,
                    "action_name": action_name,
                    "execution_status": "success",
                    "exception_id": exception_id,
                },
                context={
                    **event["context"],
                    "decision_event_id": event["event_id"],
                    "decision_offset": record.offset,
                },
                related_entities=event["related_entities"],
                confidence=min(1.0, event["confidence"] + 0.01),
            )
            await self.bus.publish(self.topic_out_executed, executed_event)

            if exception_id:
                await self.state_store.resolve_exception(
                    exception_id,
                    {"status": "resolved", "resolution_action_id": action_id},
                )
                resolved_event = build_event(
                    event_type="exception.resolved",
                    source=self.name,
                    entity_type=event["entity"]["type"],
                    entity_id=event["entity"]["id"],
                    severity="low",
                    text=f"Exception {exception_id} marked resolved",
                    signals={
                        "exception_id": exception_id,
                        "resolution_action_id": action_id,
                    },
                    context={
                        **event["context"],
                        "resolved_by_action": action_name,
                    },
                    related_entities=event["related_entities"],
                    confidence=0.99,
                )
                await self.bus.publish(self.topic_out_resolved, resolved_event)
