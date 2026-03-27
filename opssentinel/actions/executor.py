from __future__ import annotations

import logging
from typing import Any, Dict, List

from opssentinel.state.store import StateStore


class ActionService:
    """Executes actions and stores webhook-ready action records."""

    def __init__(self, store: StateStore) -> None:
        self.executed_actions: List[Dict[str, Any]] = []
        self.store = store
        self._logger = logging.getLogger("opssentinel.actions")

    async def handle_action(self, action_event: Dict[str, Any]) -> None:
        trace = dict(action_event.get("trace", {}))
        trace_id = trace.get("trace_id")

        record = {
            "action": action_event["action"],
            "target": action_event["target"],
            "message": action_event["message"],
            "exception_id": action_event["exception_id"],
            "entity": action_event["entity"],
            "event_id": action_event["event_id"],
            "trace": trace,
            "webhook": {
                "method": "POST",
                "path": "/v1/actions/execute",
                "payload": {
                    "action": action_event["action"],
                    "target": action_event["target"],
                    "message": action_event["message"],
                    "trace_id": trace_id,
                    "correlation_id": trace.get("correlation_id"),
                },
            },
        }
        self.executed_actions.append(record)

        if trace_id:
            await self.store.append_trace(
                trace_id,
                "action.executed",
                {
                    "event_id": action_event["event_id"],
                    "exception_id": action_event["exception_id"],
                    "action": action_event["action"],
                    "target": action_event["target"],
                },
            )

        self._logger.info(
            "action executed action=%s target=%s exception_id=%s trace_id=%s",
            record["action"],
            record["target"],
            record["exception_id"],
            trace_id,
        )
        print(
            f"[ACTION] action={record['action']} target={record['target']} "
            f"exception_id={record['exception_id']} trace_id={trace_id} message={record['message']}"
        )
