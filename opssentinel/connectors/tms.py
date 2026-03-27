from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict
from uuid import uuid4

from opssentinel.event_bus.bus import EventBus

SUPPORTED_TMS_PROVIDERS = {"mock", "easypost", "aftership", "shippo"}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class TMSConnector:
    """TMS ingestion microservice (mock-first, API-ready provider abstraction)."""

    def __init__(
        self,
        bus: EventBus,
        *,
        provider: str = "mock",
        interval_seconds: float = 2.0,
        total_events: int = 8,
    ) -> None:
        provider_name = provider.lower().strip()
        if provider_name not in SUPPORTED_TMS_PROVIDERS:
            raise ValueError(f"unsupported TMS provider: {provider}")

        self.bus = bus
        self.provider = provider_name
        self.interval_seconds = interval_seconds
        self.total_events = max(1, total_events)
        self._logger = logging.getLogger("opssentinel.connector.tms")
        self._sequence = 0

    def _next_id(self) -> str:
        self._sequence += 1
        return f"evt_tms_{self.provider}_{self._sequence:06d}"

    def _source_name(self) -> str:
        if self.provider == "mock":
            return "tms_mock"
        return f"tms_{self.provider}"

    def _build_event(self, index: int) -> Dict[str, Any]:
        shipment_id = "shipment-009" if index % 2 == 0 else "shipment-010"
        order_id = "order-5501" if shipment_id == "shipment-009" else "order-8802"
        trace_id = f"trc_tms_{shipment_id}"

        # Use schema-compatible event types while retaining provider-native status in signals.
        if index % 6 == 0:
            event_type = "movement_resumed"
            status = "in_transit"
            delay_minutes = 0
        elif index % 3 == 0:
            event_type = "customer_complaint"
            status = "exception"
            delay_minutes = 35
        else:
            event_type = "delay_detected"
            status = "delayed"
            delay_minutes = 30 + (index % 5) * 10

        return {
            "event_id": self._next_id(),
            "event_type": event_type,
            "source": self._source_name(),
            "timestamp": utc_now_iso(),
            "entity": {"type": "shipment", "id": shipment_id},
            "related_entities": [
                {"type": "truck", "id": "TRK12" if shipment_id == "shipment-009" else "TRK44"},
                {"type": "order", "id": order_id},
            ],
            "severity": "medium",
            "text": f"TMS update from {self.provider}: status={status}",
            "signals": {
                "shipment_id": shipment_id,
                "order_id": order_id,
                "tracking_status": status,
                "delay_minutes": delay_minutes,
                "provider": self.provider,
                "shipment_priority": "high" if shipment_id == "shipment-009" else "normal",
                "sla_sensitivity": "high" if shipment_id == "shipment-009" else "medium",
                "blast_radius": 4 if shipment_id == "shipment-009" else 2,
            },
            "context": {
                "provider": self.provider,
                "connector": "tms",
            },
            "confidence": 0.92,
            "schema_version": "1.0",
            "trace": {
                "trace_id": trace_id,
                "span_id": f"spn_tms_{uuid4().hex[:10]}",
                "parent_span_id": None,
                "stage": "connector.tms.raw",
                "correlation_id": f"corr_shipment_{shipment_id}",
            },
        }

    async def run(self) -> None:
        self._logger.info(
            "tms connector started provider=%s total_events=%d interval_seconds=%.2f",
            self.provider,
            self.total_events,
            self.interval_seconds,
        )
        for idx in range(1, self.total_events + 1):
            await asyncio.sleep(self.interval_seconds)
            event = self._build_event(idx)
            self._logger.info(
                "emit provider=%s event_id=%s type=%s shipment=%s",
                self.provider,
                event["event_id"],
                event["event_type"],
                event["entity"]["id"],
            )
            await self.bus.publish("raw_events", event)
