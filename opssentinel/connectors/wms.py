from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict
from uuid import uuid4

from opssentinel.event_bus.bus import EventBus


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class WMSConnector:
    """Warehouse signal connector (mock Odoo/WMS style events)."""

    def __init__(
        self,
        bus: EventBus,
        *,
        provider: str = "mock_wms",
        interval_seconds: float = 3.0,
        total_events: int = 4,
    ) -> None:
        self.bus = bus
        self.provider = provider
        self.interval_seconds = interval_seconds
        self.total_events = max(1, total_events)
        self._logger = logging.getLogger("opssentinel.connector.wms")
        self._sequence = 0

    def _next_id(self) -> str:
        self._sequence += 1
        return f"evt_wms_{self._sequence:06d}"

    def _build_event(self, index: int) -> Dict[str, Any]:
        shipment_id = "shipment-009" if index % 2 == 1 else "shipment-010"
        order_id = "order-5501" if shipment_id == "shipment-009" else "order-8802"

        if index % 2 == 1:
            issue_type = "inventory_mismatch"
            message = "Warehouse count mismatch detected"
            severity = "high"
        else:
            issue_type = "pick_delay"
            message = "Warehouse picking delay detected"
            severity = "medium"

        return {
            "event_id": self._next_id(),
            "event_type": "customer_complaint",
            "source": self.provider,
            "timestamp": utc_now_iso(),
            "entity": {"type": "order", "id": order_id},
            "related_entities": [
                {"type": "shipment", "id": shipment_id},
                {"type": "truck", "id": "TRK12" if shipment_id == "shipment-009" else "TRK44"},
            ],
            "severity": severity,
            "text": message,
            "signals": {
                "shipment_id": shipment_id,
                "order_id": order_id,
                "issue_type": issue_type,
                "provider": self.provider,
                "shipment_priority": "high" if shipment_id == "shipment-009" else "normal",
                "sla_sensitivity": "high" if shipment_id == "shipment-009" else "medium",
                "blast_radius": 3,
            },
            "context": {
                "provider": self.provider,
                "connector": "wms",
            },
            "confidence": 0.9,
            "schema_version": "1.0",
            "trace": {
                "trace_id": f"trc_wms_{shipment_id}",
                "span_id": f"spn_wms_{uuid4().hex[:10]}",
                "parent_span_id": None,
                "stage": "connector.wms.raw",
                "correlation_id": f"corr_shipment_{shipment_id}",
            },
        }

    async def run(self) -> None:
        self._logger.info(
            "wms connector started provider=%s total_events=%d interval_seconds=%.2f",
            self.provider,
            self.total_events,
            self.interval_seconds,
        )
        for idx in range(1, self.total_events + 1):
            await asyncio.sleep(self.interval_seconds)
            event = self._build_event(idx)
            self._logger.info(
                "emit provider=%s event_id=%s issue=%s",
                self.provider,
                event["event_id"],
                event["signals"].get("issue_type"),
            )
            await self.bus.publish("raw_events", event)
