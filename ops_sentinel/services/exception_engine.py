from __future__ import annotations

import uuid

from ops_sentinel.bus.event_bus import LocalKafkaBus
from ops_sentinel.contracts.event_contract import build_event
from ops_sentinel.services.base import BaseService
from ops_sentinel.state_store.state_store import StateStore


class ExceptionEngine(BaseService):
    """Service 6: detects exceptions from normalized event stream."""

    def __init__(
        self,
        bus: LocalKafkaBus,
        state_store: StateStore,
        topic_in: str,
        topic_out: str,
        consumer_group: str = "exception-engine-v1",
    ) -> None:
        super().__init__(name="exception-engine")
        self.bus = bus
        self.state_store = state_store
        self.topic_in = topic_in
        self.topic_out = topic_out
        self.consumer_group = consumer_group

    def _is_exception(self, event_type: str, severity: str) -> bool:
        keywords = ["delay", "complaint", "stalled", "breach", "failed"]
        return severity in {"high", "critical"} or any(k in event_type for k in keywords)

    async def run(self) -> None:
        while self.running:
            record = await self.bus.consume(self.topic_in, self.consumer_group)
            event = record.event
            if not self._is_exception(event["event_type"], event["severity"]):
                continue

            exception_id = f"exc_{uuid.uuid4().hex[:12]}"
            await self.state_store.record_exception(
                exception_id,
                {
                    "status": "open",
                    "source_event_id": event["event_id"],
                    "entity": event["entity"],
                    "severity": event["severity"],
                    "text": event["text"],
                },
            )

            ex_event = build_event(
                event_type="exception.detected",
                source=self.name,
                entity_type=event["entity"]["type"],
                entity_id=event["entity"]["id"],
                severity=event["severity"],
                text=f"Exception detected: {event['text']}",
                signals={**event["signals"], "exception_id": exception_id},
                context={
                    **event["context"],
                    "normalized_event_id": event["event_id"],
                    "normalized_offset": record.offset,
                    "exception_id": exception_id,
                },
                related_entities=event["related_entities"],
                confidence=min(1.0, event["confidence"] + 0.05),
            )
            await self.bus.publish(self.topic_out, ex_event)
