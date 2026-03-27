from __future__ import annotations

import logging
from datetime import datetime
from math import atan2, cos, radians, sin, sqrt
from typing import Any, Dict, List, Tuple
from uuid import uuid4

from opssentinel.event_bus.bus import EventBus
from opssentinel.llm.openai_client import LLMDecisionEngine
from opssentinel.mcp import MCPGateway, MCPToolError
from opssentinel.state.store import StateStore, utc_now_iso

SEVERITY_ORDER = ["low", "medium", "high", "critical"]


def _iso_to_dt(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _distance_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius_miles = 3958.8
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return radius_miles * c


def _ensure_shared_context(event: Dict[str, Any]) -> Dict[str, Any]:
    shared = dict(event.get("shared_context", {}))
    shared.setdefault("event", dict(event))
    shared.setdefault("classification", None)
    shared.setdefault("severity", None)
    shared.setdefault("decision", None)
    shared.setdefault("history", [])
    return shared


def _append_history(shared: Dict[str, Any], stage: str, stage_input: Dict[str, Any], stage_output: Dict[str, Any]) -> None:
    shared["history"].append(
        {
            "stage": stage,
            "timestamp": utc_now_iso(),
            "input": stage_input,
            "output": stage_output,
        }
    )


class DetectionAgent:
    """Agent 1: detect exception signals from raw operational events."""

    def __init__(
        self,
        bus: EventBus,
        store: StateStore,
        stationary_threshold_sec: int = 18,
        llm_engine: LLMDecisionEngine | None = None,
        mcp_gateway: MCPGateway | None = None,
    ) -> None:
        self.bus = bus
        self.store = store
        self.stationary_threshold_sec = stationary_threshold_sec
        self.llm_engine = llm_engine
        self.mcp_gateway = mcp_gateway
        self._logger = logging.getLogger("opssentinel.agent.detection")

    def detect(
        self,
        event: Dict[str, Any],
        current_state: Dict[str, Any],
    ) -> Dict[str, Any]:
        event_type = event["event_type"]
        signals = event.get("signals", {})

        if event_type != "location_update":
            is_exception = event_type in {"delay_detected", "customer_complaint"}
            return {
                "is_exception": is_exception,
                "derived_event_type": event_type,
                "reason": "non-location event forwarded",
                "delay_minutes": int(signals.get("delay_minutes", 0) or 0),
                "resolution_signal": event_type == "movement_resumed",
            }

        lat = float(signals.get("lat", 0.0))
        lon = float(signals.get("lon", 0.0))
        speed = float(signals.get("speed", 0.0))

        prev_lat = current_state.get("last_detection_lat")
        prev_lon = current_state.get("last_detection_lon")
        prev_ts = current_state.get("last_detection_ts")
        prev_stationary = float(current_state.get("stationary_seconds", 0.0) or 0.0)
        delay_active = bool(current_state.get("delay_active", False))

        stationary_seconds = 0.0
        if prev_lat is not None and prev_lon is not None and prev_ts:
            miles = _distance_miles(float(prev_lat), float(prev_lon), lat, lon)
            elapsed = max(0.0, (_iso_to_dt(event["timestamp"]) - _iso_to_dt(prev_ts)).total_seconds())
            if speed <= 1.0 and miles <= 0.03:
                stationary_seconds = prev_stationary + elapsed
            elif speed <= 1.0:
                stationary_seconds = elapsed

        delay_minutes = int(stationary_seconds // 60)

        if speed <= 1.0 and stationary_seconds >= self.stationary_threshold_sec:
            return {
                "is_exception": True,
                "derived_event_type": "delay_detected",
                "reason": f"Truck stationary above threshold ({int(stationary_seconds)}s)",
                "delay_minutes": max(1, delay_minutes),
                "resolution_signal": False,
                "delay_active": True,
                "stationary_seconds": stationary_seconds,
            }

        if speed > 3.0 and delay_active:
            return {
                "is_exception": False,
                "derived_event_type": "movement_resumed",
                "reason": "movement resumed after delay",
                "delay_minutes": 0,
                "resolution_signal": True,
                "delay_active": False,
                "stationary_seconds": 0.0,
            }

        return {
            "is_exception": False,
            "derived_event_type": "location_update",
            "reason": "no exception detected",
            "delay_minutes": delay_minutes,
            "resolution_signal": False,
            "delay_active": delay_active,
            "stationary_seconds": stationary_seconds,
        }

    async def handle_normalized_event(self, event: Dict[str, Any]) -> None:
        entity_id = event["entity"]["id"]
        trace = dict(event.get("trace", {}))
        trace_id = trace.get("trace_id", f"trc_{uuid4().hex[:16]}")
        correlation_id = trace.get("correlation_id", f"corr_entity_{entity_id}")

        current_state = await self.store.get_entity_state(entity_id)
        detection = self.detect(event, current_state)
        llm_used = False
        if self.llm_engine and self.llm_engine.enabled:
            llm_context = {
                "event": event,
                "current_state": current_state,
                "threshold_sec": self.stationary_threshold_sec,
                "shared_context": event.get("shared_context", {}),
            }
            llm_detection = await self.llm_engine.propose_detection(llm_context)
            if llm_detection:
                detection = {
                    **detection,
                    **llm_detection,
                }
                llm_used = True
            else:
                self.llm_engine.record_fallback()

        signals = dict(event.get("signals", {}))
        if detection.get("derived_event_type") == "delay_detected":
            signals["delay_minutes"] = max(int(signals.get("delay_minutes", 0) or 0), detection["delay_minutes"])

        await self.store.update_entity_state(
            entity_id,
            {
                "last_detection_lat": signals.get("lat", current_state.get("last_detection_lat")),
                "last_detection_lon": signals.get("lon", current_state.get("last_detection_lon")),
                "last_detection_ts": event["timestamp"],
                "stationary_seconds": detection.get("stationary_seconds", 0.0),
                "delay_active": detection.get("delay_active", current_state.get("delay_active", False)),
            },
        )

        shared = _ensure_shared_context(event)
        _append_history(
            shared,
            "detection",
            {
                "event_type": event["event_type"],
                "speed": signals.get("speed"),
                "lat": signals.get("lat"),
                "lon": signals.get("lon"),
            },
            detection,
        )

        out = {
            **event,
            "event_type": detection["derived_event_type"],
            "signals": signals,
            "detection": detection,
            "shared_context": shared,
            "trace": {
                "trace_id": trace_id,
                "span_id": f"spn_{uuid4().hex[:12]}",
                "parent_span_id": trace.get("span_id"),
                "correlation_id": correlation_id,
                "stage": "agent.detection",
                "causation_event_id": event["event_id"],
            },
        }

        await self.store.append_trace(
            trace_id,
            "agent.detection",
            {
                "event_id": event["event_id"],
                "derived_event_type": detection["derived_event_type"],
                "is_exception": detection["is_exception"],
                "reason": detection["reason"],
                "correlation_id": correlation_id,
                "llm_used": llm_used,
            },
        )

        self._logger.info(
            "detection input_event=%s output_event=%s is_exception=%s trace_id=%s",
            event["event_type"],
            detection["derived_event_type"],
            detection["is_exception"],
            trace_id,
        )
        await self.bus.publish("detected_events", out)


class ClassificationAgent:
    """Agent 2: deterministic exception classification."""

    def __init__(
        self,
        bus: EventBus,
        store: StateStore,
        llm_engine: LLMDecisionEngine | None = None,
        mcp_gateway: MCPGateway | None = None,
    ) -> None:
        self.bus = bus
        self.store = store
        self.llm_engine = llm_engine
        self.mcp_gateway = mcp_gateway
        self._logger = logging.getLogger("opssentinel.agent.classification")

    @staticmethod
    def classify(event_type: str, history: List[Dict[str, Any]], detection: Dict[str, Any]) -> str:
        if event_type == "delay_detected":
            return "delay"
        if event_type == "movement_resumed":
            return "delay"
        if event_type == "customer_complaint":
            return "missing" if any(e.get("event_type") == "delay_detected" for e in history) else "damage"
        if event_type == "location_update" and not detection.get("is_exception", False):
            return "false_positive"
        return "misroute"

    async def handle_detected_event(self, event: Dict[str, Any]) -> None:
        entity_id = event["entity"]["id"]
        history = await self.store.get_history(entity_id)
        detection = dict(event.get("detection", {}))
        classification = self.classify(event["event_type"], history, detection)
        llm_used = False
        if self.llm_engine and self.llm_engine.enabled:
            llm_context = {
                "event": event,
                "history_tail": history[-20:],
                "shared_context": event.get("shared_context", {}),
            }
            llm_result = await self.llm_engine.propose_classification(llm_context)
            if llm_result:
                classification = llm_result["classification"]
                llm_used = True
            else:
                self.llm_engine.record_fallback()

        shared = _ensure_shared_context(event)
        shared["classification"] = classification
        _append_history(
            shared,
            "classification",
            {
                "event_type": event["event_type"],
                "is_exception": detection.get("is_exception"),
            },
            {"classification": classification},
        )

        trace = dict(event.get("trace", {}))
        trace_id = trace.get("trace_id", f"trc_{uuid4().hex[:16]}")
        correlation_id = trace.get("correlation_id", f"corr_entity_{entity_id}")

        out = {
            **event,
            "classification": classification,
            "shared_context": shared,
            "trace": {
                "trace_id": trace_id,
                "span_id": f"spn_{uuid4().hex[:12]}",
                "parent_span_id": trace.get("span_id"),
                "correlation_id": correlation_id,
                "stage": "agent.classification",
                "causation_event_id": event["event_id"],
            },
        }

        await self.store.append_trace(
            trace_id,
            "agent.classification",
            {
                "event_id": event["event_id"],
                "classification": classification,
                "correlation_id": correlation_id,
                "llm_used": llm_used,
            },
        )

        self._logger.info(
            "classification input_event=%s output_classification=%s trace_id=%s",
            event["event_type"],
            classification,
            trace_id,
        )
        await self.bus.publish("classified_events", out)


class PrioritizationAgent:
    """Agent 3: deterministic prioritization / severity assignment."""

    def __init__(
        self,
        bus: EventBus,
        store: StateStore,
        llm_engine: LLMDecisionEngine | None = None,
        mcp_gateway: MCPGateway | None = None,
    ) -> None:
        self.bus = bus
        self.store = store
        self.llm_engine = llm_engine
        self.mcp_gateway = mcp_gateway
        self._logger = logging.getLogger("opssentinel.agent.prioritization")

    @staticmethod
    def _bump_severity(severity: str, steps: int) -> str:
        idx = SEVERITY_ORDER.index(severity)
        return SEVERITY_ORDER[min(len(SEVERITY_ORDER) - 1, idx + steps)]

    def prioritize(
        self,
        event: Dict[str, Any],
        classification: str,
        correlation_history: List[Dict[str, Any]],
    ) -> Tuple[str, str]:
        signals = event.get("signals", {})
        detection = event.get("detection", {})
        context = event.get("context", {}).get("entity_context", {})
        event_type = event["event_type"]

        if event_type == "movement_resumed":
            return "low", "resolution event"

        if classification == "false_positive":
            return "low", "no actionable exception"

        base = "medium"
        delay_minutes = int(signals.get("delay_minutes", detection.get("delay_minutes", 0)) or 0)
        if classification == "delay" and delay_minutes > 60:
            base = "high"

        customer_priority = str(context.get("customer_priority", signals.get("shipment_priority", "normal"))).lower()
        sla_sensitivity = str(context.get("sla_sensitivity", signals.get("sla_sensitivity", "medium"))).lower()
        blast_radius = int(context.get("blast_radius", signals.get("blast_radius", 1)) or 1)
        route_remaining = float(signals.get("route_remaining_ratio", 0.5) or 0.5)

        increments = 0
        reasons: List[str] = []

        if customer_priority == "high":
            increments += 1
            reasons.append("high customer priority")
        if sla_sensitivity == "high":
            increments += 1
            reasons.append("high SLA sensitivity")
        if blast_radius >= 4:
            increments += 1
            reasons.append("high blast radius")
        if route_remaining > 0.65 and classification == "delay":
            increments += 1
            reasons.append("delay far from destination")
        if len(correlation_history) >= 6:
            increments += 1
            reasons.append("persistent correlated disruptions")

        severity = self._bump_severity(base, increments)

        if classification == "delay" and delay_minutes > 60 and customer_priority == "high":
            return "critical", "delay > 60 with high priority shipment"

        reason = ", ".join(reasons) if reasons else "baseline prioritization"
        return severity, reason

    async def _mcp_enrich(
        self,
        event: Dict[str, Any],
        classification: str,
        severity: str,
        reason: str,
    ) -> Tuple[str, str, Dict[str, Any]]:
        if not self.mcp_gateway:
            return severity, reason, {}
        if classification == "false_positive" or event.get("event_type") == "movement_resumed":
            return severity, reason, {}

        signals = event.get("signals", {})
        enrichments: Dict[str, Any] = {}

        try:
            geo = await self.mcp_gateway.call_tool(
                "geo.route_risk",
                {
                    "route_remaining_ratio": signals.get("route_remaining_ratio", 0.5),
                    "speed": signals.get("speed", 0.0),
                },
            )
            enrichments["geo"] = geo
            if float(geo.get("route_risk_score", 0.0) or 0.0) >= 0.8:
                severity = self._bump_severity(severity, 1)
                reason = f"{reason}, high route risk via MCP"
        except MCPToolError as exc:
            self._logger.warning("mcp geo enrichment failed: %s", exc)

        try:
            profile = await self.mcp_gateway.call_tool(
                "tms.shipment_profile",
                {"shipment_id": signals.get("shipment_id", "")},
            )
            enrichments["tms"] = profile
            if profile.get("customer_tier") == "enterprise":
                severity = self._bump_severity(severity, 1)
                reason = f"{reason}, enterprise shipment tier via MCP"
        except MCPToolError as exc:
            self._logger.warning("mcp tms enrichment failed: %s", exc)

        issue_type = signals.get("issue_type")
        if issue_type:
            try:
                wms = await self.mcp_gateway.call_tool("wms.order_risk", {"issue_type": issue_type})
                enrichments["wms"] = wms
                if bool(wms.get("fulfillment_blocked")):
                    severity = self._bump_severity(severity, 1)
                    reason = f"{reason}, fulfillment blocked via MCP"
            except MCPToolError as exc:
                self._logger.warning("mcp wms enrichment failed: %s", exc)

        return severity, reason, enrichments

    async def handle_classified_event(self, event: Dict[str, Any]) -> None:
        entity_id = event["entity"]["id"]
        classification = event["classification"]

        trace = dict(event.get("trace", {}))
        trace_id = trace.get("trace_id", f"trc_{uuid4().hex[:16]}")
        correlation_id = trace.get("correlation_id", f"corr_entity_{entity_id}")

        correlation_history = await self.store.get_correlation_history(correlation_id)
        severity, reason = self.prioritize(event, classification, correlation_history)
        severity, reason, mcp_enrichment = await self._mcp_enrich(event, classification, severity, reason)
        llm_used = False
        if self.llm_engine and self.llm_engine.enabled:
            llm_context = {
                "event": event,
                "classification": classification,
                "correlation_history_tail": correlation_history[-30:],
                "shared_context": event.get("shared_context", {}),
            }
            llm_result = await self.llm_engine.propose_prioritization(llm_context)
            if llm_result:
                severity = llm_result["severity"]
                reason = llm_result["reason"]
                llm_used = True
            else:
                self.llm_engine.record_fallback()

        shared = _ensure_shared_context(event)
        shared["severity"] = severity
        _append_history(
            shared,
            "prioritization",
            {
                "classification": classification,
                "delay_minutes": event.get("signals", {}).get("delay_minutes"),
            },
            {"severity": severity, "reason": reason},
        )

        out = {
            **event,
            "severity": severity,
            "prioritization_reason": reason,
            "mcp_enrichment": mcp_enrichment,
            "shared_context": shared,
            "trace": {
                "trace_id": trace_id,
                "span_id": f"spn_{uuid4().hex[:12]}",
                "parent_span_id": trace.get("span_id"),
                "correlation_id": correlation_id,
                "stage": "agent.prioritization",
                "causation_event_id": event["event_id"],
            },
        }

        await self.store.append_trace(
            trace_id,
            "agent.prioritization",
            {
                "event_id": event["event_id"],
                "classification": classification,
                "severity": severity,
                "reason": reason,
                "mcp_enrichment": mcp_enrichment,
                "correlation_id": correlation_id,
                "llm_used": llm_used,
            },
        )

        self._logger.info(
            "prioritization input_classification=%s output_severity=%s trace_id=%s",
            classification,
            severity,
            trace_id,
        )
        await self.bus.publish("prioritized_events", out)


class DecisionAgent:
    """Agent 4: deterministic policy decision."""

    def __init__(
        self,
        bus: EventBus,
        store: StateStore,
        llm_engine: LLMDecisionEngine | None = None,
        mcp_gateway: MCPGateway | None = None,
    ) -> None:
        self.bus = bus
        self.store = store
        self.llm_engine = llm_engine
        self.mcp_gateway = mcp_gateway
        self._logger = logging.getLogger("opssentinel.agent.decision")

    @staticmethod
    def decide(event_type: str, classification: str, severity: str) -> str:
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
    def decision_reason(
        event_type: str,
        classification: str,
        severity: str,
        prioritization_reason: str,
    ) -> str:
        if event_type == "movement_resumed":
            return "movement resumed, closing active delay exception"
        if classification == "false_positive":
            return "no action, keep monitoring"
        return f"policy mapping {classification}/{severity}; factors: {prioritization_reason}"

    async def handle_prioritized_event(self, event: Dict[str, Any]) -> None:
        classification = event["classification"]
        severity = event["severity"]
        event_type = event["event_type"]
        prioritization_reason = event.get("prioritization_reason", "baseline prioritization")
        decision = self.decide(event_type, classification, severity)
        reason = self.decision_reason(event_type, classification, severity, prioritization_reason)

        llm_used = False
        if self.llm_engine and self.llm_engine.enabled:
            llm_context = {
                "event_type": event_type,
                "classification": classification,
                "severity": severity,
                "prioritization_reason": prioritization_reason,
                "signals": event.get("signals", {}),
                "shared_context": event.get("shared_context", {}),
            }
            llm_result = await self.llm_engine.propose(llm_context)
            if llm_result:
                classification = llm_result["classification"]
                severity = llm_result["severity"]
                decision = llm_result["decision"]
                reason = llm_result["reason"]
                llm_used = True
            else:
                self.llm_engine.record_fallback()

        entity_id = event["entity"]["id"]
        trace = dict(event.get("trace", {}))
        trace_id = trace.get("trace_id", f"trc_{uuid4().hex[:16]}")
        correlation_id = trace.get("correlation_id", f"corr_entity_{entity_id}")

        shared = _ensure_shared_context(event)
        shared["decision"] = decision
        _append_history(
            shared,
            "decision",
            {
                "classification": classification,
                "severity": severity,
            },
            {"decision": decision, "reason": reason},
        )

        payload = {
            "event_id": event["event_id"],
            "entity": event["entity"],
            "related_entities": event.get("related_entities", []),
            "event_type": event_type,
            "result": {
                "classification": classification,
                "severity": severity,
                "decision": decision,
                "reason": reason,
                "llm_used": llm_used,
            },
            "signals": event.get("signals", {}),
            "context": event.get("context", {}),
            "timestamp": event.get("timestamp"),
            "shared_context": shared,
            "trace": {
                "trace_id": trace_id,
                "span_id": f"spn_{uuid4().hex[:12]}",
                "parent_span_id": trace.get("span_id"),
                "correlation_id": correlation_id,
                "stage": "agent.decision",
                "causation_event_id": event["event_id"],
            },
        }

        await self.store.append_trace(
            trace_id,
            "agent.decision",
            {
                "event_id": event["event_id"],
                "classification": classification,
                "severity": severity,
                "decision": decision,
                "correlation_id": correlation_id,
                "llm_used": llm_used,
            },
        )

        self._logger.info(
            "decision input=%s/%s output=%s trace_id=%s",
            classification,
            severity,
            decision,
            trace_id,
        )
        await self.bus.publish("decisions", payload)
