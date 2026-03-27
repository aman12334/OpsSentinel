from __future__ import annotations

import asyncio
import os
from typing import Any, Dict

from opssentinel.mcp.gateway import MCPGateway


def _has_any_env(*keys: str) -> bool:
    for key in keys:
        value = os.getenv(key)
        if value and value.strip():
            return True
    return False


def build_default_mcp_gateway() -> MCPGateway:
    gateway = MCPGateway()

    async def geo_route_risk(args: Dict[str, Any]) -> Dict[str, Any]:
        await asyncio.sleep(0)
        route_remaining = float(args.get("route_remaining_ratio", 0.5) or 0.5)
        speed = float(args.get("speed", 0.0) or 0.0)

        provider = "mock"
        if _has_any_env("MAPBOX_API_KEY"):
            provider = "mapbox"
        elif _has_any_env("GRAPHHOPPER_API_KEY"):
            provider = "graphhopper"
        elif _has_any_env("GOOGLE_MAPS_API_KEY"):
            provider = "google_maps"

        score = route_remaining
        if speed <= 1.0:
            score = min(1.0, score + 0.25)
        elif speed < 20.0:
            score = min(1.0, score + 0.10)

        return {
            "provider": provider,
            "route_risk_score": round(max(0.0, min(score, 1.0)), 2),
            "eta_delta_minutes": int(10 + route_remaining * 40),
        }

    async def tms_shipment_profile(args: Dict[str, Any]) -> Dict[str, Any]:
        await asyncio.sleep(0)
        shipment_id = str(args.get("shipment_id") or "")

        provider = "mock"
        if _has_any_env("EASYPOST_API_KEY"):
            provider = "easypost"
        elif _has_any_env("AFTERSHIP_API_KEY"):
            provider = "aftership"
        elif _has_any_env("SHIPPO_API_KEY"):
            provider = "shippo"

        high = shipment_id.endswith("9") or shipment_id.endswith("1")
        return {
            "provider": provider,
            "customer_tier": "enterprise" if high else "standard",
            "priority": "high" if high else "normal",
            "sla_sensitivity": "high" if high else "medium",
        }

    async def wms_order_risk(args: Dict[str, Any]) -> Dict[str, Any]:
        await asyncio.sleep(0)
        issue_type = str(args.get("issue_type") or "")
        if issue_type == "inventory_mismatch":
            return {"provider": "mock_wms", "warehouse_risk": "high", "fulfillment_blocked": True}
        if issue_type == "pick_delay":
            return {"provider": "mock_wms", "warehouse_risk": "medium", "fulfillment_blocked": False}
        return {"provider": "mock_wms", "warehouse_risk": "low", "fulfillment_blocked": False}

    gateway.register_tool("geo.route_risk", geo_route_risk)
    gateway.register_tool("tms.shipment_profile", tms_shipment_profile)
    gateway.register_tool("wms.order_risk", wms_order_risk)
    return gateway
