from __future__ import annotations

import asyncio
import random
from typing import List, Tuple

from ops_sentinel.bus.event_bus import LocalKafkaBus
from ops_sentinel.contracts.event_contract import build_event
from ops_sentinel.services.base import BaseService


class EventProducer(BaseService):
    """Service 1: event producer simulation."""

    def __init__(self, bus: LocalKafkaBus, topic_out: str, max_events: int = 40, interval_s: float = 0.15) -> None:
        super().__init__(name="event-producer")
        self.bus = bus
        self.topic_out = topic_out
        self.max_events = max_events
        self.interval_s = interval_s
        self.trucks = [f"truck-{i:03d}" for i in range(1, 6)]
        self.shipments = [f"shp-{i:04d}" for i in range(1001, 1011)]
        self.orders = [f"ord-{i:05d}" for i in range(20001, 20011)]

    def _pick_entity(self) -> Tuple[str, str]:
        choice = random.choice(["truck", "shipment", "order"])
        if choice == "truck":
            return choice, random.choice(self.trucks)
        if choice == "shipment":
            return choice, random.choice(self.shipments)
        return choice, random.choice(self.orders)

    def _next_event(self) -> dict:
        entity_type, entity_id = self._pick_entity()
        template = random.choice(
            [
                ("gps_stalled", "medium", "GPS indicates stopped movement for 45 minutes"),
                ("shipment_delay_update", "high", "Carrier ETA has slipped by 3 hours"),
                ("customer_complaint", "high", "Customer reports package not delivered"),
                ("temperature_breach", "critical", "Cold chain temperature above threshold"),
                ("movement_resumed", "low", "Vehicle movement resumed after interruption"),
            ]
        )
        event_type, severity, text = template
        signals = {
            "speed_kph": random.randint(0, 90),
            "eta_delta_minutes": random.randint(-30, 240),
            "temperature_c": round(random.uniform(2.0, 11.0), 2),
        }
        context = {
            "region": random.choice(["NE", "SE", "MW", "SW", "NW"]),
            "route_risk": random.choice(["low", "medium", "high"]),
        }
        related_entities: List[dict] = []
        if entity_type != "shipment":
            related_entities.append({"type": "shipment", "id": random.choice(self.shipments)})

        return build_event(
            event_type=event_type,
            source="simulation",
            entity_type=entity_type,  # type: ignore[arg-type]
            entity_id=entity_id,
            severity=severity,  # type: ignore[arg-type]
            text=text,
            signals=signals,
            context=context,
            related_entities=related_entities,
            confidence=round(random.uniform(0.7, 0.99), 2),
        )

    async def run(self) -> None:
        for _ in range(self.max_events):
            if not self.running:
                break
            event = self._next_event()
            await self.bus.publish(self.topic_out, event)
            await asyncio.sleep(self.interval_s)
