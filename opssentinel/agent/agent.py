from __future__ import annotations

import logging
from typing import Any, Dict, Tuple
from uuid import uuid4

from opssentinel.event_bus.bus import EventBus
from opssentinel.state.store import StateStore


SEVERITY_ORDER = ["low", "medium", "high", "critical"]


class AgentService:
    """Deterministic decision engine."""

    def __init__(self, bus: EventBus, store: StateStore) -> None:
        self.bus = bus
        self.store = store
        self._logger = logging.getLogger("opssentinel.agent")

    def evaluate(
        self,
        event: Dict[str, Any],
        current_state: Dict[str, Any],
        history: list[Dict[str, Any]],
        correlation_history: list[Dict[str, Any]],
    ) -> Dict[str, str]:
        event_type = event["event_type"]
        signals = event.get("signals", {})
        entity_context = event.get("context", {}).get("entity_context", {})

        delay_minutes = int(signals.get("delay_minutes", 0) or 0)
        priority = str(signals.get("shipment_priority", entity_context.get("customer_priority", "normal"))).lower()

        classification, base_severity = self._classify_base(event_type, delay_minutes, priority, history)
        severity, impact_reason = self._apply_business_impact(
            base_severity=base_severity,
            event_type=event_type,
            classification=classification,
            signals=signals,
            entity_context=entity_context,
            correlation_history=correlation_history,
        )
        decision = self._decide(event_type, classification, severity)
        reason = self._reason(event_type, classification, severity, delay_minutes, priority, impact_reason)

        return {
            "classification": classification,
            "severity": severity,
            "decision": decision,
            "reason": reason,
        }

    @staticmethod
    def _classify_base(
        event_type: str,
        delay_minutes: int,
        priority: str,
        history: list[Dict[str, Any]],
    ) -> Tuple[str, str]:
        if event_type == "delay_detected":
            classification = "delay"
            if delay_minutes > 60 and priority == "high":
                return classification, "critical"
            if delay_minutes > 60:
                return classification, "high"
            if delay_minutes <= 5:
                return "false_positive", "low"
            return classification, "medium"

        if event_type == "movement_resumed":
            return "delay", "low"

        if event_type == "customer_complaint":
            complaint_type = "missing" if any(e.get("event_type") == "delay_detected" for e in history) else "damage"
            return complaint_type, "medium"

        return "misroute", "low"

    @staticmethod
    def _bump_severity(severity: str, steps: int) -> str:
        idx = SEVERITY_ORDER.index(severity)
        return SEVERITY_ORDER[min(len(SEVERITY_ORDER) - 1, idx + steps)]

    def _apply_business_impact(
        self,
        base_severity: str,
        event_type: str,
        classification: str,
        signals: Dict[str, Any],
        entity_context: Dict[str, Any],
        correlation_history: list[Dict[str, Any]],
    ) -> Tuple[str, str]:
        if event_type == "movement_resumed" or classification == "false_positive":
            return "low", "closure or false positive signal"

        customer_priority = str(entity_context.get("customer_priority", "normal")).lower()
        sla_sensitivity = str(entity_context.get("sla_sensitivity", "medium")).lower()
        blast_radius = int(entity_context.get("blast_radius", 1) or 1)

        increments = 0
        reasons = []
        if customer_priority == "high":
            increments += 1
            reasons.append("high customer priority")
        if sla_sensitivity == "high":
            increments += 1
            reasons.append("high SLA sensitivity")
        if blast_radius >= 4:
            increments += 1
            reasons.append("high blast radius")

        if len(correlation_history) >= 4:
            increments += 1
            reasons.append("persistent correlated signal pattern")

        # Keep explicit base rule deterministic and dominant.
        if event_type == "delay_detected" and int(signals.get("delay_minutes", 0) or 0) > 60 and customer_priority == "high":
            return "critical", "delay > 60 with high priority shipment"

        severity = self._bump_severity(base_severity, increments)
        reason = ", ".join(reasons) if reasons else "baseline severity"
        return severity, reason

    @staticmethod
    def _decide(event_type: str, classification: str, severity: str) -> str:
        if event_type == "movement_resumed":
            return "close"
        if classification == "false_positive":
            return "wait"
        if severity in {"critical", "high"}:
            return "escalate"
        if classification in {"missing", "damage"}:
            return "notify_customer"
        if classification == "delay":
            return "notify_ops"
        return "wait"

    @staticmethod
    def _reason(
        event_type: str,
        classification: str,
        severity: str,
        delay_minutes: int,
        priority: str,
        impact_reason: str,
    ) -> str:
        if event_type == "movement_resumed":
            return "movement resumed, closing active delay exception"
        if classification == "false_positive":
            return "delay signal likely noise, monitor only"
        if event_type == "delay_detected" and delay_minutes > 60 and priority == "high":
            return "delay > 60 minutes with high priority shipment"
        if event_type == "delay_detected" and delay_minutes > 60:
            return f"delay > 60 minutes; impact factors: {impact_reason}"
        return f"deterministic policy matched for {classification} with {severity} severity; impact factors: {impact_reason}"

    async def handle_normalized_event(self, event: Dict[str, Any]) -> None:
        entity_id = event["entity"]["id"]
        trace = dict(event.get("trace", {}))
        trace_id = trace.get("trace_id", f"trc_{uuid4().hex[:16]}")
        correlation_id = trace.get("correlation_id", f"corr_entity_{entity_id}")

        current_state = await self.store.get_entity_state(entity_id)
        history = await self.store.get_history(entity_id)
        correlation_history = await self.store.get_correlation_history(correlation_id)

        result = self.evaluate(event, current_state, history, correlation_history)

        decision_trace = {
            "trace_id": trace_id,
            "span_id": f"spn_{uuid4().hex[:12]}",
            "parent_span_id": trace.get("span_id"),
            "correlation_id": correlation_id,
            "stage": "agent.decision",
            "causation_event_id": event["event_id"],
        }

        payload = {
            "event_id": event["event_id"],
            "entity": event["entity"],
            "related_entities": event.get("related_entities", []),
            "event_type": event["event_type"],
            "result": result,
            "signals": event.get("signals", {}),
            "context": event.get("context", {}),
            "timestamp": event.get("timestamp"),
            "trace": decision_trace,
        }

        await self.store.append_trace(
            trace_id,
            "agent.decision",
            {
                "event_id": event["event_id"],
                "classification": result["classification"],
                "severity": result["severity"],
                "decision": result["decision"],
                "correlation_id": correlation_id,
            },
        )

        self._logger.info(
            "decision produced event_id=%s classification=%s decision=%s trace_id=%s",
            event["event_id"],
            result["classification"],
            result["decision"],
            trace_id,
        )
        await self.bus.publish("decisions", payload)
