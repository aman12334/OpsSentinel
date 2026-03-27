from __future__ import annotations

import asyncio
import json
import pathlib
import sys
from typing import Any, Dict, List

import streamlit as st

ROOT = pathlib.Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from opssentinel.runtime import run_pipeline


st.set_page_config(page_title="OpsSentinel Control Center", layout="wide")

st.title("OpsSentinel Stakeholder Dashboard")
st.caption("LLM-powered multi-agent logistics exception orchestration")

with st.sidebar:
    st.header("Run Controls")
    ticks = st.slider("Simulation Ticks", min_value=6, max_value=60, value=14, step=2)
    tick_seconds = st.slider("Tick Seconds", min_value=0.5, max_value=3.0, value=1.2, step=0.1)
    stationary_threshold = st.slider("Stationary Threshold (sec)", min_value=2, max_value=60, value=3, step=1)
    llm_enabled = st.toggle("Enable LLM Decision Agent", value=False)
    llm_model = st.text_input("LLM Model", value="gpt-4.1-mini")
    mcp_enabled = st.toggle("Enable MCP Tool Gateway", value=True)
    tms_provider = st.selectbox(
        "TMS Feed",
        options=["none", "mock", "easypost", "aftership", "shippo"],
        index=0,
    )
    wms_enabled = st.toggle("Enable WMS Feed", value=False)
    run_clicked = st.button("Run Simulation", type="primary", use_container_width=True)

if "result" not in st.session_state:
    st.session_state["result"] = None

if run_clicked:
    with st.spinner("Running multi-agent simulation..."):
        try:
            st.session_state["result"] = asyncio.run(
                run_pipeline(
                    tick_seconds=tick_seconds,
                    total_ticks=ticks,
                    stationary_threshold_sec=stationary_threshold,
                    llm_enabled=llm_enabled,
                    llm_model=llm_model,
                    mcp_enabled=mcp_enabled,
                    tms_provider=tms_provider,
                    wms_enabled=wms_enabled,
                )
            )
        except Exception as exc:  # noqa: BLE001
            st.error(f"Run failed: {exc}")

result = st.session_state["result"]

if not result:
    st.info("Configure controls and click 'Run Simulation' to generate a stakeholder-ready run.")
    st.stop()

snapshot = result["snapshot"]
actions = result["actions"]
exceptions = snapshot.get("exceptions", {})
entities = snapshot.get("entities", {})
traces = snapshot.get("traces", {})
metrics = snapshot.get("metrics", {})

col1, col2, col3, col4 = st.columns(4)
col1.metric("Entities", len(entities))
col2.metric("Exceptions", len(exceptions))
col3.metric("Actions", len(actions))
col4.metric(
    "LLM/MCP",
    f"{'LLM on' if result.get('llm_enabled') else 'LLM off'} | {'MCP on' if result.get('mcp_enabled') else 'MCP off'}",
)

st.subheader("LLM Quality Metrics")
m1, m2, m3, m4 = st.columns(4)
m1.metric("llm_calls_total", int(metrics.get("llm_calls_total", 0)))
m2.metric("llm_calls_valid", int(metrics.get("llm_calls_valid", 0)))
m3.metric("llm_calls_invalid", int(metrics.get("llm_calls_invalid", 0)))
m4.metric("llm_fallbacks_used", int(metrics.get("llm_fallbacks_used", 0)))

st.subheader("Action Feed")
if actions:
    st.dataframe(actions, use_container_width=True)
else:
    st.write("No actions emitted in this run.")

st.subheader("Open/Resolved Exceptions")
if exceptions:
    st.dataframe(list(exceptions.values()), use_container_width=True)
else:
    st.write("No exceptions created.")

st.subheader("Truck Geo Snapshot")
map_rows: List[Dict[str, Any]] = []
for entity_id, state in entities.items():
    if state.get("entity", {}).get("type") != "truck":
        continue
    lat = state.get("last_detection_lat")
    lon = state.get("last_detection_lon")
    if lat is None or lon is None:
        continue
    map_rows.append({"lat": float(lat), "lon": float(lon), "truck_id": entity_id})

if map_rows:
    st.map(map_rows)
else:
    st.write("No geo points available.")

st.subheader("Trace Explorer")
trace_ids = sorted(traces.keys())
if trace_ids:
    selected = st.selectbox("Trace ID", trace_ids)
    st.json(traces[selected])
else:
    st.write("No traces recorded.")

st.subheader("Raw Snapshot")
with st.expander("View full JSON snapshot"):
    st.code(json.dumps(snapshot, indent=2), language="json")
