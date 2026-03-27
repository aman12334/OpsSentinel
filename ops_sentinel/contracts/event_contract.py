from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, TypedDict
import uuid

EntityType = Literal["truck", "shipment", "order"]
Severity = Literal["low", "medium", "high", "critical"]

ALLOWED_ENTITY_TYPES = {"truck", "shipment", "order"}
ALLOWED_SEVERITY = {"low", "medium", "high", "critical"}
SCHEMA_VERSION = "1.0"


class Entity(TypedDict):
    type: EntityType
    id: str


class Event(TypedDict):
    event_id: str
    event_type: str
    source: str
    timestamp: str
    entity: Entity
    related_entities: List[Dict[str, str]]
    severity: Severity
    text: str
    signals: Dict[str, Any]
    context: Dict[str, Any]
    confidence: float
    schema_version: str


class EventValidationError(ValueError):
    pass


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def new_event_id(prefix: str = "evt") -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def validate_event(event: Dict[str, Any]) -> Event:
    required = {
        "event_id",
        "event_type",
        "source",
        "timestamp",
        "entity",
        "related_entities",
        "severity",
        "text",
        "signals",
        "context",
        "confidence",
        "schema_version",
    }

    keys = set(event.keys())
    missing = sorted(required - keys)
    extra = sorted(keys - required)
    if missing:
        raise EventValidationError(f"Missing required fields: {', '.join(missing)}")
    if extra:
        raise EventValidationError(f"Unknown fields not allowed: {', '.join(extra)}")

    for field in ["event_id", "event_type", "source", "timestamp", "text", "schema_version"]:
        if not isinstance(event[field], str) or not event[field].strip():
            raise EventValidationError(f"Field '{field}' must be a non-empty string")

    if event["schema_version"] != SCHEMA_VERSION:
        raise EventValidationError(
            f"schema_version must be '{SCHEMA_VERSION}', got '{event['schema_version']}'"
        )

    entity = event["entity"]
    if not isinstance(entity, dict):
        raise EventValidationError("entity must be an object")
    if set(entity.keys()) != {"type", "id"}:
        raise EventValidationError("entity must contain exactly 'type' and 'id'")
    if entity["type"] not in ALLOWED_ENTITY_TYPES:
        raise EventValidationError(f"entity.type must be one of {sorted(ALLOWED_ENTITY_TYPES)}")
    if not isinstance(entity["id"], str) or not entity["id"].strip():
        raise EventValidationError("entity.id must be a non-empty string")

    related = event["related_entities"]
    if not isinstance(related, list):
        raise EventValidationError("related_entities must be an array")
    for i, rel in enumerate(related):
        if not isinstance(rel, dict):
            raise EventValidationError(f"related_entities[{i}] must be an object")
        if set(rel.keys()) != {"type", "id"}:
            raise EventValidationError(
                f"related_entities[{i}] must contain exactly 'type' and 'id'"
            )
        if rel["type"] not in ALLOWED_ENTITY_TYPES:
            raise EventValidationError(
                f"related_entities[{i}].type must be one of {sorted(ALLOWED_ENTITY_TYPES)}"
            )
        if not isinstance(rel["id"], str) or not rel["id"].strip():
            raise EventValidationError(f"related_entities[{i}].id must be a non-empty string")

    if event["severity"] not in ALLOWED_SEVERITY:
        raise EventValidationError(f"severity must be one of {sorted(ALLOWED_SEVERITY)}")

    if not isinstance(event["signals"], dict):
        raise EventValidationError("signals must be an object")
    if not isinstance(event["context"], dict):
        raise EventValidationError("context must be an object")

    confidence = event["confidence"]
    if not isinstance(confidence, (int, float)):
        raise EventValidationError("confidence must be numeric")
    if confidence < 0 or confidence > 1:
        raise EventValidationError("confidence must be in range [0.0, 1.0]")

    try:
        datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))
    except ValueError as exc:
        raise EventValidationError("timestamp must be a valid ISO datetime") from exc

    return event  # type: ignore[return-value]


def build_event(
    *,
    event_type: str,
    source: str,
    entity_type: EntityType,
    entity_id: str,
    severity: Severity,
    text: str,
    signals: Dict[str, Any] | None = None,
    context: Dict[str, Any] | None = None,
    related_entities: List[Dict[str, str]] | None = None,
    confidence: float = 1.0,
    event_id: str | None = None,
    timestamp: str | None = None,
) -> Event:
    event: Dict[str, Any] = {
        "event_id": event_id or new_event_id(),
        "event_type": event_type,
        "source": source,
        "timestamp": timestamp or utc_now_iso(),
        "entity": {"type": entity_type, "id": entity_id},
        "related_entities": related_entities or [],
        "severity": severity,
        "text": text,
        "signals": signals or {},
        "context": context or {},
        "confidence": confidence,
        "schema_version": SCHEMA_VERSION,
    }
    return validate_event(event)
