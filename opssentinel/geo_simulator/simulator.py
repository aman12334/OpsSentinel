from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from math import atan2, cos, radians, sin, sqrt
from typing import Any, Dict, List, Tuple

from opssentinel.event_bus.bus import EventBus


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def distance_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 3958.8
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return r * c


@dataclass
class TruckState:
    truck_id: str
    lat: float
    lon: float
    speed: float
    route: List[Tuple[float, float]]
    status: str
    route_index: int = 0
    stop_ticks_remaining: int = 0


class GeoSimulator:
    """Geo-spatial microservice that emits realistic truck movement events."""

    def __init__(
        self,
        bus: EventBus,
        tick_seconds: float = 2.0,
        total_ticks: int = 24,
    ) -> None:
        self.bus = bus
        self.tick_seconds = tick_seconds
        self.total_ticks = total_ticks
        self._logger = logging.getLogger("opssentinel.geo_simulator")
        self._event_seq = 0
        self._tick_idx = 0
        self.trucks = self._seed_trucks()

    def _seed_trucks(self) -> List[TruckState]:
        return [
            TruckState(
                truck_id="TRK12",
                lat=38.8977,
                lon=-77.0365,
                speed=55.0,
                route=[
                    (38.8977, -77.0365),
                    (38.9301, -77.0280),
                    (38.9650, -77.0200),
                    (39.0020, -77.0100),
                ],
                status="moving",
            ),
            TruckState(
                truck_id="TRK44",
                lat=40.7128,
                lon=-74.0060,
                speed=48.0,
                route=[
                    (40.7128, -74.0060),
                    (40.7420, -74.0100),
                    (40.7690, -74.0050),
                    (40.7900, -73.9900),
                ],
                status="moving",
            ),
        ]

    def _next_event_id(self) -> str:
        self._event_seq += 1
        return f"evt_geo_{self._event_seq:06d}"

    def _random_perturb(self, truck: TruckState) -> None:
        roll = random.random()

        # Unexpected stop / long idle
        if truck.stop_ticks_remaining <= 0 and roll < 0.2:
            truck.status = "stopped"
            truck.speed = 0.0
            truck.stop_ticks_remaining = random.randint(6, 10)
            return

        if truck.stop_ticks_remaining > 0:
            truck.stop_ticks_remaining -= 1
            if truck.stop_ticks_remaining == 0:
                truck.status = "moving"
                truck.speed = random.uniform(35.0, 58.0)
            return

        # Gradual slowdown
        if roll < 0.18:
            truck.speed = max(5.0, truck.speed - random.uniform(4.0, 12.0))
            truck.status = "delayed" if truck.speed < 20.0 else "moving"

        # Route deviation
        if roll > 0.90:
            truck.lat += random.uniform(-0.01, 0.01)
            truck.lon += random.uniform(-0.01, 0.01)
            truck.status = "delayed"

    def _apply_scenario_disruption(self, truck: TruckState) -> bool:
        # Deterministic disruptions for repeatable demos.
        scripted = {
            "TRK12": (2, 5),
            "TRK44": (7, 10),
        }
        window = scripted.get(truck.truck_id)
        if not window:
            return False
        start, end = window
        if start <= self._tick_idx <= end:
            truck.status = "stopped"
            truck.speed = 0.0
            return True
        if self._tick_idx == end + 1:
            truck.status = "moving"
            truck.speed = max(truck.speed, 45.0)
            truck.stop_ticks_remaining = 0
            return False
        return False

    def _move_truck(self, truck: TruckState) -> None:
        if truck.status == "stopped" or truck.speed <= 0.5:
            return

        next_idx = min(truck.route_index + 1, len(truck.route) - 1)
        target_lat, target_lon = truck.route[next_idx]

        dist = distance_miles(truck.lat, truck.lon, target_lat, target_lon)
        if dist < 0.03:
            truck.route_index = next_idx
            if truck.route_index >= len(truck.route) - 1:
                truck.route_index = 0
            return

        miles_per_tick = truck.speed * (self.tick_seconds / 3600.0)
        step = min(1.0, miles_per_tick / max(dist, 1e-6))
        truck.lat = truck.lat + (target_lat - truck.lat) * step
        truck.lon = truck.lon + (target_lon - truck.lon) * step

    def _route_remaining_ratio(self, truck: TruckState) -> float:
        return max(0.0, (len(truck.route) - 1 - truck.route_index) / max(1, len(truck.route) - 1))

    def _build_event(self, truck: TruckState) -> Dict[str, Any]:
        shipment_id = "shipment-009" if truck.truck_id == "TRK12" else "shipment-010"
        order_id = "order-5501" if truck.truck_id == "TRK12" else "order-8802"

        return {
            "event_id": self._next_event_id(),
            "event_type": "location_update",
            "source": "gps_feed",
            "timestamp": utc_now_iso(),
            "entity": {"type": "truck", "id": truck.truck_id},
            "related_entities": [
                {"type": "shipment", "id": shipment_id},
                {"type": "order", "id": order_id},
            ],
            "signals": {
                "lat": round(truck.lat, 6),
                "lon": round(truck.lon, 6),
                "speed": round(truck.speed, 2),
                "route_index": truck.route_index,
                "route_remaining_ratio": round(self._route_remaining_ratio(truck), 2),
                "shipment_id": shipment_id,
                "order_id": order_id,
                "shipment_priority": "high" if truck.truck_id == "TRK12" else "normal",
                "sla_sensitivity": "high" if truck.truck_id == "TRK12" else "medium",
                "blast_radius": 4 if truck.truck_id == "TRK12" else 2,
            },
            "trace": {
                "trace_id": f"trc_{truck.truck_id.lower()}",
                "span_id": f"spn_geo_{self._event_seq + 1:06d}",
                "parent_span_id": None,
                "stage": "geo_simulator.raw",
                "correlation_id": f"corr_shipment_{shipment_id}",
            },
        }

    async def run(self) -> None:
        self._logger.info("geo simulator started trucks=%d ticks=%d", len(self.trucks), self.total_ticks)
        for self._tick_idx in range(1, self.total_ticks + 1):
            await asyncio.sleep(self.tick_seconds)
            for truck in self.trucks:
                scripted_stop = self._apply_scenario_disruption(truck)
                if not scripted_stop:
                    self._random_perturb(truck)
                self._move_truck(truck)
                event = self._build_event(truck)
                self._logger.info(
                    "emit truck=%s status=%s lat=%s lon=%s speed=%s",
                    truck.truck_id,
                    truck.status,
                    event["signals"]["lat"],
                    event["signals"]["lon"],
                    event["signals"]["speed"],
                )
                await self.bus.publish("raw_events", event)
