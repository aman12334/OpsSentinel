from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import Any, Dict, List


class StateStore:
    """In-memory state store; replaceable with Redis/Postgres in production."""

    def __init__(self) -> None:
        self._entity_state: Dict[str, Dict[str, Any]] = defaultdict(dict)
        self._exceptions: Dict[str, Dict[str, Any]] = {}
        self._actions: Dict[str, Dict[str, Any]] = {}
        self._audit_log: List[Dict[str, Any]] = []
        self._lock = asyncio.Lock()

    @staticmethod
    def _entity_key(entity_type: str, entity_id: str) -> str:
        return f"{entity_type}:{entity_id}"

    async def upsert_entity_state(self, entity_type: str, entity_id: str, patch: Dict[str, Any]) -> None:
        key = self._entity_key(entity_type, entity_id)
        async with self._lock:
            self._entity_state[key].update(patch)

    async def record_exception(self, exception_id: str, record: Dict[str, Any]) -> None:
        async with self._lock:
            self._exceptions[exception_id] = record
            self._audit_log.append({"kind": "exception", "id": exception_id, **record})

    async def resolve_exception(self, exception_id: str, resolution: Dict[str, Any]) -> None:
        async with self._lock:
            current = self._exceptions.get(exception_id, {})
            current.update(resolution)
            self._exceptions[exception_id] = current
            self._audit_log.append({"kind": "resolution", "id": exception_id, **resolution})

    async def record_action(self, action_id: str, record: Dict[str, Any]) -> None:
        async with self._lock:
            self._actions[action_id] = record
            self._audit_log.append({"kind": "action", "id": action_id, **record})

    async def snapshot(self) -> Dict[str, Any]:
        async with self._lock:
            return {
                "entity_state": dict(self._entity_state),
                "exceptions": dict(self._exceptions),
                "actions": dict(self._actions),
                "audit_log": list(self._audit_log),
            }
