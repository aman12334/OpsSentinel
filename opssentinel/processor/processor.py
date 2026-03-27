from __future__ import annotations

import logging
from typing import Any, Dict, List
from uuid import uuid4

from opssentinel.event_bus.bus import EventBus
from opssentinel.state.store import StateStore, utc_now_iso


class EventProcessor:
    def __init__(self, bus: EventBus, store: StateStore) -> None:
        self.bus = bus
        self.store = store
        self._logger = logging.getLogger("opssentinel.processor")

    @staticmethod
    def _derive_correlation_id(entity: Dict[str, Any], signals: Dict[str, Any], related_entities: List[Dict[str, Any]]) -> str:
        if signals.get("shipment_id"):
            return f"corr_shipment_{signals['shipment_id']}"
        if entity.get("type") == "shipment":
            return f"corr_shipment_{entity['id']}"
        for rel in related_entities:
            if rel.get("type") == "shipment":
                return f"corr_shipment_{rel.get('id')}"
        if signals.get("order_id"):
            return f"corr_order_{signals['order_id']}"
        return f"corr_entity_{entity.get('id', 'unknown')}"

    async def handle_raw_event(self, event: Dict[str, Any]) -> None:
        entity = event["entity"]
        entity_id = entity["id"]
        event_type = event.get("event_type", "unknown")
        signals = dict(event.get("signals", {}))
        related_entities = list(event.get("related_entities", []))

        incoming_trace = dict(event.get("trace", {}))
        trace_id = incoming_trace.get("trace_id", f"trc_{uuid4().hex[:16]}")
        parent_span_id = incoming_trace.get("span_id")
        span_id = f"spn_{uuid4().hex[:12]}"
        correlation_id = self._derive_correlation_id(entity, signals, related_entities)
        entity_context = await self.store.get_entity_context(entity_id, signals)

        normalized = {
            "event_id": event["event_id"],
            "event_type": event_type,
            "timestamp": event.get("timestamp") or utc_now_iso(),
            "entity": entity,
            "related_entities": related_entities,
            "signals": signals,
            "context": {
                "source": event.get("source", "unknown"),
                "normalized_at": utc_now_iso(),
                "entity_context": entity_context,
            },
            "shared_context": {
                "event": {
                    "event_id": event["event_id"],
                    "event_type": event_type,
                    "entity": entity,
                    "signals": signals,
                },
                "classification": None,
                "severity": None,
                "decision": None,
                "history": [],
            },
            "trace": {
                "trace_id": trace_id,
                "span_id": span_id,
                "parent_span_id": parent_span_id,
                "correlation_id": correlation_id,
                "stage": "processor.normalized",
                "causation_event_id": event["event_id"],
            },
        }

        await self.store.add_entity_event(entity_id, normalized)
        await self.store.record_correlation(correlation_id, normalized, entity, related_entities)
        await self.store.update_entity_state(
            entity_id,
            {
                "entity": entity,
                "last_event_type": event_type,
                "last_event_id": event["event_id"],
                "last_event_at": normalized["timestamp"],
                "last_source": normalized["context"]["source"],
                "last_trace_id": trace_id,
                "correlation_id": correlation_id,
            },
        )
        await self.store.append_trace(
            trace_id,
            "processor.normalized",
            {
                "event_id": event["event_id"],
                "entity_id": entity_id,
                "correlation_id": correlation_id,
                "source": normalized["context"]["source"],
            },
        )

        self._logger.info(
            "raw->normalized input=%s output=normalized event_id=%s trace_id=%s correlation_id=%s",
            event_type,
            event["event_id"],
            trace_id,
            correlation_id,
        )
        await self.bus.publish("normalized_events", normalized)
