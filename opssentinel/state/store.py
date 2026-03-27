from __future__ import annotations

import asyncio
import copy
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class StateStore:
    """In-memory state store structured for production replacement."""

    def __init__(self) -> None:
        self.state: Dict[str, Dict[str, Any]] = {
            "entities": {},
            "exceptions": {},
            "correlations": {},
            "traces": {},
            "metrics": {
                "llm_calls_total": 0,
                "llm_calls_valid": 0,
                "llm_calls_invalid": 0,
                "llm_fallbacks_used": 0,
            },
        }
        self._history: Dict[str, List[Dict[str, Any]]] = {}
        self._correlation_history: Dict[str, List[Dict[str, Any]]] = {}
        self._lock = asyncio.Lock()
        self._logger = logging.getLogger("opssentinel.state")
        self._entity_profiles: Dict[str, Dict[str, Any]] = {
            "truck-001": {"customer_priority": "normal", "sla_sensitivity": "medium", "blast_radius": 2},
            "truck-002": {"customer_priority": "high", "sla_sensitivity": "high", "blast_radius": 5},
            "shipment-009": {"customer_priority": "high", "sla_sensitivity": "high", "blast_radius": 4},
            "order-5501": {"customer_priority": "high", "sla_sensitivity": "medium", "blast_radius": 3},
        }

    async def get_entity_state(self, entity_id: str) -> Dict[str, Any]:
        async with self._lock:
            return copy.deepcopy(self.state["entities"].get(entity_id, {}))

    async def update_entity_state(self, entity_id: str, data: Dict[str, Any]) -> None:
        async with self._lock:
            current = self.state["entities"].get(entity_id, {})
            current.update(copy.deepcopy(data))
            current["updated_at"] = utc_now_iso()
            self.state["entities"][entity_id] = current
        self._logger.info("entity state updated entity_id=%s", entity_id)

    async def add_entity_event(self, entity_id: str, event: Dict[str, Any]) -> None:
        async with self._lock:
            bucket = self._history.setdefault(entity_id, [])
            bucket.append(copy.deepcopy(event))
            if len(bucket) > 200:
                del bucket[: len(bucket) - 200]

    async def get_history(self, entity_id: str) -> List[Dict[str, Any]]:
        async with self._lock:
            return copy.deepcopy(self._history.get(entity_id, []))

    async def get_correlation_history(self, correlation_id: str) -> List[Dict[str, Any]]:
        async with self._lock:
            return copy.deepcopy(self._correlation_history.get(correlation_id, []))

    async def record_correlation(
        self,
        correlation_id: str,
        event: Dict[str, Any],
        entity: Dict[str, Any],
        related_entities: List[Dict[str, Any]],
    ) -> None:
        async with self._lock:
            corr = self.state["correlations"].get(correlation_id, {})
            entities = set(corr.get("entities", []))
            entities.add(f"{entity.get('type')}:{entity.get('id')}")
            for rel in related_entities:
                entities.add(f"{rel.get('type')}:{rel.get('id')}")

            event_ids = list(corr.get("event_ids", []))
            event_ids.append(event["event_id"])

            sources = set(corr.get("sources", []))
            src = event.get("context", {}).get("source")
            if src:
                sources.add(src)

            self.state["correlations"][correlation_id] = {
                "correlation_id": correlation_id,
                "entities": sorted(entities),
                "event_ids": event_ids[-300:],
                "sources": sorted(sources),
                "last_event_at": event.get("timestamp", utc_now_iso()),
                "updated_at": utc_now_iso(),
            }

            bucket = self._correlation_history.setdefault(correlation_id, [])
            bucket.append(copy.deepcopy(event))
            if len(bucket) > 300:
                del bucket[: len(bucket) - 300]

    async def get_entity_context(
        self,
        entity_id: str,
        signals: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        profile = copy.deepcopy(
            self._entity_profiles.get(
                entity_id,
                {"customer_priority": "normal", "sla_sensitivity": "medium", "blast_radius": 1},
            )
        )
        if signals:
            if signals.get("shipment_priority") in {"normal", "high"}:
                profile["customer_priority"] = signals["shipment_priority"]
            if signals.get("sla_sensitivity") in {"low", "medium", "high"}:
                profile["sla_sensitivity"] = signals["sla_sensitivity"]
            if signals.get("blast_radius") is not None:
                try:
                    profile["blast_radius"] = max(1, int(signals["blast_radius"]))
                except (TypeError, ValueError):
                    pass
        return profile

    async def append_trace(self, trace_id: str, stage: str, payload: Dict[str, Any]) -> None:
        async with self._lock:
            trace = self.state["traces"].get(trace_id, {"trace_id": trace_id, "steps": []})
            trace["steps"].append(
                {
                    "stage": stage,
                    "timestamp": utc_now_iso(),
                    "payload": copy.deepcopy(payload),
                }
            )
            self.state["traces"][trace_id] = trace

    async def create_exception(
        self,
        exception_type: str,
        entity: Dict[str, Any],
        severity: str,
        event_id: str,
        correlation_id: Optional[str] = None,
        trace_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        exception_id = f"exc_{uuid4().hex[:12]}"
        now = utc_now_iso()
        record = {
            "exception_id": exception_id,
            "type": exception_type,
            "status": "open",
            "entity": copy.deepcopy(entity),
            "linked_events": [event_id],
            "severity": severity,
            "actions_taken": [],
            "correlation_id": correlation_id,
            "trace_id": trace_id,
            "created_at": now,
            "updated_at": now,
        }
        async with self._lock:
            self.state["exceptions"][exception_id] = record
        self._logger.info("exception created exception_id=%s type=%s", exception_id, exception_type)
        return copy.deepcopy(record)

    async def update_exception(self, exception_id: str, updates: Dict[str, Any]) -> Dict[str, Any]:
        async with self._lock:
            current = self.state["exceptions"].get(exception_id)
            if current is None:
                raise KeyError(f"unknown exception_id={exception_id}")
            current.update(copy.deepcopy(updates))
            current["updated_at"] = utc_now_iso()
            self.state["exceptions"][exception_id] = current
            updated = copy.deepcopy(current)
        self._logger.info("exception updated exception_id=%s status=%s", exception_id, updated.get("status"))
        return updated

    async def find_open_exception(self, entity_id: str, exception_type: str) -> Optional[Dict[str, Any]]:
        async with self._lock:
            for exc in self.state["exceptions"].values():
                if (
                    exc.get("entity", {}).get("id") == entity_id
                    and exc.get("type") == exception_type
                    and exc.get("status") in {"open", "monitoring", "escalated"}
                ):
                    return copy.deepcopy(exc)
        return None

    async def snapshot(self) -> Dict[str, Any]:
        async with self._lock:
            return copy.deepcopy(self.state)

    async def set_llm_metrics(self, metrics: Dict[str, int]) -> None:
        async with self._lock:
            current = self.state.setdefault("metrics", {})
            current["llm_calls_total"] = int(metrics.get("llm_calls_total", 0))
            current["llm_calls_valid"] = int(metrics.get("llm_calls_valid", 0))
            current["llm_calls_invalid"] = int(metrics.get("llm_calls_invalid", 0))
            current["llm_fallbacks_used"] = int(metrics.get("llm_fallbacks_used", 0))
