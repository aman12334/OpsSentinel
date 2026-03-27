from __future__ import annotations

import logging
from typing import Any, Dict
from uuid import uuid4

from opssentinel.event_bus.bus import EventBus
from opssentinel.state.store import StateStore


class ExceptionEngine:
    def __init__(self, bus: EventBus, store: StateStore) -> None:
        self.bus = bus
        self.store = store
        self._logger = logging.getLogger("opssentinel.exception")

    async def handle_decision(self, decision_event: Dict[str, Any]) -> None:
        entity = decision_event["entity"]
        entity_id = entity["id"]
        result = decision_event["result"]
        event_id = decision_event["event_id"]
        trace = dict(decision_event.get("trace", {}))
        trace_id = trace.get("trace_id")
        correlation_id = trace.get("correlation_id", f"corr_entity_{entity_id}")

        exception_type = result["classification"]
        decision = result["decision"]
        severity = result["severity"]

        # Do not create operational noise exceptions for benign location updates.
        if exception_type == "false_positive" and decision == "wait":
            self._logger.info(
                "exception skipped classification=%s decision=%s event_id=%s",
                exception_type,
                decision,
                event_id,
            )
            return

        open_exception = await self.store.find_open_exception(entity_id, exception_type)

        if decision == "close":
            if open_exception is not None:
                linked = list(open_exception.get("linked_events", []))
                linked.append(event_id)
                updated = await self.store.update_exception(
                    open_exception["exception_id"],
                    {
                        "status": "resolved",
                        "linked_events": linked,
                        "severity": severity,
                        "trace_id": trace_id,
                    },
                )
                exception_record = updated
            else:
                exception_record = await self.store.create_exception(
                    exception_type,
                    entity,
                    severity,
                    event_id,
                    correlation_id=correlation_id,
                    trace_id=trace_id,
                )
                exception_record = await self.store.update_exception(
                    exception_record["exception_id"],
                    {"status": "resolved", "trace_id": trace_id},
                )
        else:
            status = "open"
            if decision == "escalate":
                status = "escalated"
            elif decision == "wait":
                status = "monitoring"

            if open_exception is None:
                exception_record = await self.store.create_exception(
                    exception_type,
                    entity,
                    severity,
                    event_id,
                    correlation_id=correlation_id,
                    trace_id=trace_id,
                )
                exception_record = await self.store.update_exception(
                    exception_record["exception_id"],
                    {"status": status, "trace_id": trace_id},
                )
            else:
                linked = list(open_exception.get("linked_events", []))
                linked.append(event_id)
                exception_record = await self.store.update_exception(
                    open_exception["exception_id"],
                    {
                        "status": status,
                        "linked_events": linked,
                        "severity": severity,
                        "trace_id": trace_id,
                    },
                )

        action_message = self._build_action_message(decision_event, exception_record)
        actions_taken = list(exception_record.get("actions_taken", []))
        event_type = decision_event.get("event_type")
        duplicate_action = (
            bool(actions_taken)
            and actions_taken[-1] == action_message["action"]
            and event_type == "delay_detected"
            and action_message["action"] in {"wait", "notify_ops", "notify_customer", "escalate"}
        )

        if duplicate_action:
            self._logger.info(
                "action skipped duplicate exception_id=%s action=%s event_id=%s",
                exception_record["exception_id"],
                action_message["action"],
                event_id,
            )
            return

        actions_taken.append(action_message["action"])
        await self.store.update_exception(
            exception_record["exception_id"],
            {"actions_taken": actions_taken},
        )

        if trace_id:
            await self.store.append_trace(
                trace_id,
                "exception.lifecycle",
                {
                    "event_id": event_id,
                    "exception_id": exception_record["exception_id"],
                    "status": exception_record["status"],
                    "action": action_message["action"],
                    "correlation_id": correlation_id,
                },
            )

        self._logger.info(
            "exception updated exception_id=%s status=%s action=%s trace_id=%s",
            exception_record["exception_id"],
            exception_record["status"],
            action_message["action"],
            trace_id,
        )
        await self.bus.publish("actions", action_message)

    @staticmethod
    def _build_action_message(decision_event: Dict[str, Any], exception_record: Dict[str, Any]) -> Dict[str, Any]:
        result = decision_event["result"]
        decision = result["decision"]
        severity = result["severity"]
        classification = result["classification"]
        trace = dict(decision_event.get("trace", {}))

        target = "ops_queue"
        if decision == "escalate":
            target = "ops_manager"
        elif decision == "notify_customer":
            target = "customer_success"

        message = "no-op"
        if classification == "delay" and severity in {"high", "critical"}:
            message = "Truck delayed > 60 min"
        elif decision == "close":
            message = "Issue resolved by movement resume"
        elif classification in {"missing", "damage"}:
            message = "Customer complaint requires outreach"
        elif classification == "false_positive":
            message = "Signal likely false positive, keep monitoring"
        else:
            message = "Monitoring active exception"

        return {
            "action": decision,
            "target": target,
            "message": message,
            "exception_id": exception_record["exception_id"],
            "entity": decision_event["entity"],
            "event_id": decision_event["event_id"],
            "trace": {
                "trace_id": trace.get("trace_id"),
                "span_id": f"spn_{uuid4().hex[:12]}",
                "parent_span_id": trace.get("span_id"),
                "correlation_id": trace.get("correlation_id"),
                "stage": "action.requested",
                "causation_event_id": decision_event["event_id"],
            },
        }
