from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict

from opssentinel.actions.executor import ActionService
from opssentinel.agent.multi_agents import (
    ClassificationAgent,
    DecisionAgent,
    DetectionAgent,
    PrioritizationAgent,
)
from opssentinel.connectors import TMSConnector, WMSConnector
from opssentinel.event_bus.bus import EventBus
from opssentinel.exception.engine import ExceptionEngine
from opssentinel.geo_simulator.simulator import GeoSimulator
from opssentinel.llm.openai_client import LLMDecisionEngine
from opssentinel.processor.processor import EventProcessor
from opssentinel.state.store import StateStore


async def run_pipeline(
    *,
    tick_seconds: float = 1.2,
    total_ticks: int = 14,
    stationary_threshold_sec: int = 3,
    llm_enabled: bool = False,
    llm_model: str | None = None,
    tms_provider: str = "none",
    wms_enabled: bool = False,
) -> Dict[str, Any]:
    bus = EventBus()
    store = StateStore()

    llm_engine = LLMDecisionEngine(model=llm_model) if llm_enabled else None
    if llm_enabled and (not llm_engine or not llm_engine.enabled):
        raise RuntimeError("LLM mode requested but OPENAI_API_KEY is not configured.")

    processor = EventProcessor(bus, store)
    detection_agent = DetectionAgent(
        bus,
        store,
        stationary_threshold_sec=stationary_threshold_sec,
        llm_engine=llm_engine,
    )
    classification_agent = ClassificationAgent(bus, store, llm_engine=llm_engine)
    prioritization_agent = PrioritizationAgent(bus, store, llm_engine=llm_engine)
    decision_agent = DecisionAgent(bus, store, llm_engine=llm_engine)
    exception_engine = ExceptionEngine(bus, store)
    action_service = ActionService(store)
    geo_simulator = GeoSimulator(bus, tick_seconds=tick_seconds, total_ticks=total_ticks)
    tms_connector = (
        TMSConnector(
            bus,
            provider=tms_provider,
            interval_seconds=max(0.5, tick_seconds * 1.4),
            total_events=max(4, total_ticks // 2),
        )
        if tms_provider != "none"
        else None
    )
    wms_connector = (
        WMSConnector(
            bus,
            interval_seconds=max(0.8, tick_seconds * 1.8),
            total_events=max(2, total_ticks // 4),
        )
        if wms_enabled
        else None
    )

    bus.subscribe("raw_events", processor.handle_raw_event)
    bus.subscribe("normalized_events", detection_agent.handle_normalized_event)
    bus.subscribe("detected_events", classification_agent.handle_detected_event)
    bus.subscribe("classified_events", prioritization_agent.handle_classified_event)
    bus.subscribe("prioritized_events", decision_agent.handle_prioritized_event)
    bus.subscribe("decisions", exception_engine.handle_decision)
    bus.subscribe("actions", action_service.handle_action)

    logging.getLogger("opssentinel.runtime").info(
        "run pipeline ticks=%d tick_seconds=%.2f llm_enabled=%s llm_model=%s tms_provider=%s wms_enabled=%s",
        total_ticks,
        tick_seconds,
        bool(llm_engine and llm_engine.enabled),
        llm_model,
        tms_provider,
        wms_enabled,
    )

    tasks = [asyncio.create_task(geo_simulator.run(), name="geo-simulator")]
    if tms_connector:
        tasks.append(asyncio.create_task(tms_connector.run(), name=f"tms-{tms_provider}"))
    if wms_connector:
        tasks.append(asyncio.create_task(wms_connector.run(), name="wms-connector"))
    await asyncio.gather(*tasks)
    await asyncio.sleep(1.0)
    await bus.drain()

    if llm_engine:
        await store.set_llm_metrics(llm_engine.get_metrics())

    snapshot = await store.snapshot()
    await bus.shutdown()

    return {
        "snapshot": snapshot,
        "actions": action_service.executed_actions,
        "llm_enabled": bool(llm_engine and llm_engine.enabled),
        "llm_model": llm_engine.model if llm_engine else None,
        "llm_metrics": snapshot.get("metrics", {}),
        "tms_provider": tms_provider,
        "wms_enabled": wms_enabled,
    }
