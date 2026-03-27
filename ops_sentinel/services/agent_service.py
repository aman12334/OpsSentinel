from __future__ import annotations

import uuid

from ops_sentinel.bus.event_bus import LocalKafkaBus
from ops_sentinel.contracts.event_contract import build_event
from ops_sentinel.services.base import BaseService
from ops_sentinel.state_store.state_store import StateStore


class AgentService(BaseService):
    """Service 4: decision engine that maps exceptions to actions."""

    def __init__(
        self,
        bus: LocalKafkaBus,
        state_store: StateStore,
        topic_in: str,
        topic_out: str,
        consumer_group: str = "agent-service-v1",
    ) -> None:
        super().__init__(name="agent-service")
        self.bus = bus
        self.state_store = state_store
        self.topic_in = topic_in
        self.topic_out = topic_out
        self.consumer_group = consumer_group

    @staticmethod
    def _action_for(severity: str, event_type: str) -> str:
        if severity == "critical":
            return "escalate_war_room"
        if "complaint" in event_type:
            return "notify_customer_success"
        if "delay" in event_type or severity == "high":
            return "reroute_and_update_eta"
        return "monitor"

    async def run(self) -> None:
        while self.running:
            record = await self.bus.consume(self.topic_in, self.consumer_group)
            event = record.event
            action_name = self._action_for(event["severity"], event["event_type"])
            action_id = f"act_{uuid.uuid4().hex[:12]}"
            exception_id = event["context"].get("exception_id")
            decision = build_event(
                event_type="action.decided",
                source=self.name,
                entity_type=event["entity"]["type"],
                entity_id=event["entity"]["id"],
                severity=event["severity"],
                text=f"Action decided: {action_name}",
                signals={
                    "action_id": action_id,
                    "action_name": action_name,
                    "exception_id": exception_id,
                },
                context={
                    **event["context"],
                    "decision_source_event": event["event_id"],
                    "action_id": action_id,
                    "action_name": action_name,
                },
                related_entities=event["related_entities"],
                confidence=min(1.0, event["confidence"] + 0.03),
            )
            await self.state_store.record_action(
                action_id,
                {
                    "status": "decided",
                    "exception_id": exception_id,
                    "entity": event["entity"],
                    "action_name": action_name,
                    "severity": event["severity"],
                },
            )
            await self.bus.publish(self.topic_out, decision)
