from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List
from uuid import uuid4

from opssentinel.event_bus.bus import EventBus


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class EventSimulator:
    def __init__(self, bus: EventBus) -> None:
        self.bus = bus
        self._logger = logging.getLogger("opssentinel.producer")

    async def run(self) -> None:
        timeline = self._timeline()
        for item in timeline:
            await asyncio.sleep(item["after_s"])
            event = item["event"]
            self._logger.info(
                "produce source=%s event_id=%s type=%s trace_id=%s",
                event["source"],
                event["event_id"],
                event["event_type"],
                event["trace"]["trace_id"],
            )
            await self.bus.publish("raw_events", event)

    def _timeline(self) -> List[Dict[str, Any]]:
        trace_ship_009 = "trc_ship_009"
        trace_truck_002 = "trc_truck_002"

        return [
            {
                "after_s": 0.1,
                "event": self._event(
                    event_id="evt_gps_001",
                    event_type="delay_detected",
                    source="gps_feed",
                    entity={"type": "truck", "id": "truck-001"},
                    related_entities=[{"type": "shipment", "id": "shipment-009"}],
                    trace_id=trace_ship_009,
                    signals={
                        "shipment_id": "shipment-009",
                        "order_id": "order-5501",
                        "delay_minutes": 45,
                        "shipment_priority": "normal",
                        "sla_sensitivity": "medium",
                        "blast_radius": 2,
                    },
                ),
            },
            {
                "after_s": 0.2,
                "event": self._event(
                    event_id="evt_track_002",
                    event_type="delay_detected",
                    source="shipment_tracking",
                    entity={"type": "shipment", "id": "shipment-009"},
                    related_entities=[
                        {"type": "truck", "id": "truck-001"},
                        {"type": "order", "id": "order-5501"},
                    ],
                    trace_id=trace_ship_009,
                    signals={
                        "shipment_id": "shipment-009",
                        "delay_minutes": 80,
                        "shipment_priority": "high",
                        "sla_sensitivity": "high",
                        "blast_radius": 4,
                    },
                ),
            },
            {
                "after_s": 0.2,
                "event": self._event(
                    event_id="evt_ticket_003",
                    event_type="customer_complaint",
                    source="customer_ticket",
                    entity={"type": "order", "id": "order-5501"},
                    related_entities=[{"type": "shipment", "id": "shipment-009"}],
                    trace_id=trace_ship_009,
                    signals={
                        "shipment_id": "shipment-009",
                        "order_id": "order-5501",
                        "complaint_channel": "email",
                        "shipment_priority": "high",
                        "sla_sensitivity": "high",
                        "blast_radius": 4,
                    },
                ),
            },
            {
                "after_s": 0.2,
                "event": self._event(
                    event_id="evt_hist_004",
                    event_type="delay_detected",
                    source="historical_log",
                    entity={"type": "truck", "id": "truck-002"},
                    related_entities=[{"type": "shipment", "id": "shipment-010"}],
                    trace_id=trace_truck_002,
                    signals={
                        "shipment_id": "shipment-010",
                        "delay_minutes": 95,
                        "shipment_priority": "high",
                        "sla_sensitivity": "high",
                        "blast_radius": 5,
                    },
                ),
            },
            {
                "after_s": 0.2,
                "event": self._event(
                    event_id="evt_gps_005",
                    event_type="movement_resumed",
                    source="gps_feed",
                    entity={"type": "truck", "id": "truck-001"},
                    related_entities=[{"type": "shipment", "id": "shipment-009"}],
                    trace_id=trace_ship_009,
                    signals={
                        "shipment_id": "shipment-009",
                        "delay_minutes": 0,
                        "shipment_priority": "normal",
                    },
                ),
            },
            {
                "after_s": 0.2,
                "event": self._event(
                    event_id="evt_gps_006",
                    event_type="movement_resumed",
                    source="gps_feed",
                    entity={"type": "truck", "id": "truck-002"},
                    related_entities=[{"type": "shipment", "id": "shipment-010"}],
                    trace_id=trace_truck_002,
                    signals={
                        "shipment_id": "shipment-010",
                        "delay_minutes": 0,
                        "shipment_priority": "high",
                    },
                ),
            },
        ]

    @staticmethod
    def _event(
        event_id: str,
        event_type: str,
        source: str,
        entity: Dict[str, str],
        related_entities: List[Dict[str, str]],
        trace_id: str,
        signals: Dict[str, Any],
    ) -> Dict[str, Any]:
        return {
            "event_id": event_id,
            "event_type": event_type,
            "source": source,
            "timestamp": utc_now_iso(),
            "entity": entity,
            "related_entities": related_entities,
            "signals": signals,
            "trace": {
                "trace_id": trace_id,
                "span_id": f"spn_raw_{uuid4().hex[:10]}",
                "parent_span_id": None,
                "stage": "producer.raw",
                "correlation_id": f"corr_shipment_{signals.get('shipment_id', entity['id'])}",
            },
        }
