from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
from typing import Any, Dict, Optional

from openai import OpenAI

ALLOWED_CLASSIFICATIONS = {"delay", "misroute", "missing", "damage", "false_positive"}
ALLOWED_SEVERITIES = {"low", "medium", "high", "critical"}
ALLOWED_DECISIONS = {"notify_ops", "notify_customer", "escalate", "wait", "close"}
ALLOWED_EVENT_TYPES = {"location_update", "delay_detected", "movement_resumed", "customer_complaint"}


class LLMDecisionEngine:
    """LLM-backed decision proposer with strict schema validation."""

    def __init__(self, model: Optional[str] = None) -> None:
        self.model = model or os.getenv("OPSSENTINEL_LLM_MODEL", "gpt-4.1-mini")
        self._logger = logging.getLogger("opssentinel.llm")
        self._api_key = os.getenv("OPENAI_API_KEY")
        self._client: Optional[OpenAI] = OpenAI(api_key=self._api_key) if self._api_key else None
        self._metrics_lock = threading.Lock()
        self._metrics: Dict[str, int] = {
            "llm_calls_total": 0,
            "llm_calls_valid": 0,
            "llm_calls_invalid": 0,
            "llm_fallbacks_used": 0,
        }

    @property
    def enabled(self) -> bool:
        return bool(self._client)

    async def propose(self, context: Dict[str, Any]) -> Optional[Dict[str, str]]:
        if not self._client:
            return None
        return await self._call_stage(self._propose_sync, context)

    async def propose_detection(self, context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not self._client:
            return None
        return await self._call_stage(self._propose_detection_sync, context)

    async def propose_classification(self, context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not self._client:
            return None
        return await self._call_stage(self._propose_classification_sync, context)

    async def propose_prioritization(self, context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not self._client:
            return None
        return await self._call_stage(self._propose_prioritization_sync, context)

    async def _call_stage(self, fn: Any, context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            result = await asyncio.to_thread(fn, context)
        except Exception:  # noqa: BLE001
            self._logger.exception("LLM stage execution failed")
            self._record_call(valid=False)
            return None
        self._record_call(valid=bool(result))
        return result

    def _record_call(self, *, valid: bool) -> None:
        with self._metrics_lock:
            self._metrics["llm_calls_total"] += 1
            if valid:
                self._metrics["llm_calls_valid"] += 1
            else:
                self._metrics["llm_calls_invalid"] += 1

    def record_fallback(self) -> None:
        with self._metrics_lock:
            self._metrics["llm_fallbacks_used"] += 1

    def get_metrics(self) -> Dict[str, int]:
        with self._metrics_lock:
            return dict(self._metrics)

    def _propose_sync(self, context: Dict[str, Any]) -> Optional[Dict[str, str]]:
        assert self._client is not None

        system_prompt = (
            "You are OpsSentinel decision agent. Return only strict JSON with keys: "
            "classification, severity, decision, reason. "
            "Use only these enums: classification in [delay, misroute, missing, damage, false_positive], "
            "severity in [low, medium, high, critical], "
            "decision in [notify_ops, notify_customer, escalate, wait, close]."
        )

        user_prompt = json.dumps(context, separators=(",", ":"))

        try:
            response = self._client.responses.create(
                model=self.model,
                temperature=0,
                input=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
            )
        except Exception:
            self._logger.exception("LLM call failed")
            return None

        text = getattr(response, "output_text", "") or ""
        if not text:
            return None

        parsed = self._extract_json(text)
        if not parsed:
            return None

        classification = parsed.get("classification")
        severity = parsed.get("severity")
        decision = parsed.get("decision")
        reason = parsed.get("reason")

        if classification not in ALLOWED_CLASSIFICATIONS:
            return None
        if severity not in ALLOWED_SEVERITIES:
            return None
        if decision not in ALLOWED_DECISIONS:
            return None
        if not isinstance(reason, str) or not reason.strip():
            return None

        return {
            "classification": classification,
            "severity": severity,
            "decision": decision,
            "reason": reason.strip(),
        }

    def _propose_detection_sync(self, context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        assert self._client is not None
        system_prompt = (
            "You are OpsSentinel Detection Agent. Return strict JSON with keys: "
            "is_exception (bool), derived_event_type (location_update|delay_detected|movement_resumed|customer_complaint), "
            "reason (string), delay_minutes (int), resolution_signal (bool), delay_active (bool)."
        )
        parsed = self._request_json(system_prompt, context)
        if not parsed:
            return None
        if not isinstance(parsed.get("is_exception"), bool):
            return None
        if parsed.get("derived_event_type") not in ALLOWED_EVENT_TYPES:
            return None
        if not isinstance(parsed.get("reason"), str) or not parsed["reason"].strip():
            return None
        if not isinstance(parsed.get("resolution_signal"), bool):
            return None
        if not isinstance(parsed.get("delay_active"), bool):
            return None
        try:
            delay_minutes = int(parsed.get("delay_minutes", 0))
        except Exception:
            return None
        parsed["delay_minutes"] = max(0, delay_minutes)
        return parsed

    def _propose_classification_sync(self, context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        assert self._client is not None
        system_prompt = (
            "You are OpsSentinel Classification Agent. Return strict JSON with key "
            "classification in [delay, misroute, missing, damage, false_positive]."
        )
        parsed = self._request_json(system_prompt, context)
        if not parsed:
            return None
        classification = parsed.get("classification")
        if classification not in ALLOWED_CLASSIFICATIONS:
            return None
        return {"classification": classification}

    def _propose_prioritization_sync(self, context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        assert self._client is not None
        system_prompt = (
            "You are OpsSentinel Prioritization Agent. Return strict JSON with keys: "
            "severity (low|medium|high|critical), reason (string)."
        )
        parsed = self._request_json(system_prompt, context)
        if not parsed:
            return None
        severity = parsed.get("severity")
        reason = parsed.get("reason")
        if severity not in ALLOWED_SEVERITIES:
            return None
        if not isinstance(reason, str) or not reason.strip():
            return None
        return {"severity": severity, "reason": reason.strip()}

    def _request_json(self, system_prompt: str, context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        assert self._client is not None
        try:
            response = self._client.responses.create(
                model=self.model,
                temperature=0,
                input=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": json.dumps(context, separators=(",", ":"))},
                ],
            )
        except Exception:
            self._logger.exception("LLM call failed")
            return None
        text = getattr(response, "output_text", "") or ""
        if not text:
            return None
        return self._extract_json(text)

    @staticmethod
    def _extract_json(text: str) -> Optional[Dict[str, Any]]:
        text = text.strip()
        if not text:
            return None

        if text.startswith("```"):
            text = text.strip("`")
            if text.startswith("json"):
                text = text[4:]
            text = text.strip()

        try:
            obj = json.loads(text)
            if isinstance(obj, dict):
                return obj
        except json.JSONDecodeError:
            pass

        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or end <= start:
            return None

        try:
            obj = json.loads(text[start : end + 1])
            if isinstance(obj, dict):
                return obj
        except json.JSONDecodeError:
            return None

        return None
