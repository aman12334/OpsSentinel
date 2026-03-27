from __future__ import annotations

from ops_sentinel.bus.event_bus import LocalKafkaBus
from ops_sentinel.contracts.event_contract import EventValidationError, build_event, validate_event
from ops_sentinel.services.base import BaseService
from ops_sentinel.state_store.state_store import StateStore


class EventProcessorService(BaseService):
    """Service 3: validates and normalizes inbound events."""

    def __init__(
        self,
        bus: LocalKafkaBus,
        state_store: StateStore,
        topic_in: str,
        topic_out: str,
        consumer_group: str = "event-processor-v1",
    ) -> None:
        super().__init__(name="event-processor")
        self.bus = bus
        self.state_store = state_store
        self.topic_in = topic_in
        self.topic_out = topic_out
        self.consumer_group = consumer_group

    async def run(self) -> None:
        while self.running:
            record = await self.bus.consume(self.topic_in, self.consumer_group)
            raw = record.event
            try:
                event = validate_event(raw)
            except EventValidationError:
                continue

            await self.state_store.upsert_entity_state(
                event["entity"]["type"],
                event["entity"]["id"],
                {
                    "last_event_id": event["event_id"],
                    "last_event_type": event["event_type"],
                    "last_seen_ts": event["timestamp"],
                    "last_severity": event["severity"],
                },
            )

            normalized_type = f"normalized.{event['event_type']}"
            normalized = build_event(
                event_type=normalized_type,
                source=self.name,
                entity_type=event["entity"]["type"],
                entity_id=event["entity"]["id"],
                severity=event["severity"],
                text=event["text"],
                signals=event["signals"],
                context={
                    **event["context"],
                    "ingested_from": self.topic_in,
                    "raw_event_id": event["event_id"],
                    "raw_offset": record.offset,
                },
                related_entities=event["related_entities"],
                confidence=event["confidence"],
            )
            await self.bus.publish(self.topic_out, normalized)
