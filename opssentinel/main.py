from __future__ import annotations

import argparse
import asyncio
import json
import logging
import pathlib
import sys

if __package__ is None or __package__ == "":
    sys.path.append(str(pathlib.Path(__file__).resolve().parent.parent))

from opssentinel.runtime import run_pipeline


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run OpsSentinel multi-agent simulation")
    parser.add_argument("--ticks", type=int, default=14)
    parser.add_argument("--tick-seconds", type=float, default=1.2)
    parser.add_argument("--stationary-threshold-sec", type=int, default=3)
    parser.add_argument("--llm", action="store_true", help="Enable LLM-powered decision override")
    parser.add_argument("--llm-model", type=str, default=None)
    parser.add_argument(
        "--tms-provider",
        type=str,
        default="none",
        choices=["none", "mock", "easypost", "aftership", "shippo"],
        help="Enable TMS connector with provider profile",
    )
    parser.add_argument("--wms-enabled", action="store_true", help="Enable mock WMS connector")
    return parser.parse_args()


async def _main(args: argparse.Namespace) -> None:
    configure_logging()
    result = await run_pipeline(
        tick_seconds=args.tick_seconds,
        total_ticks=args.ticks,
        stationary_threshold_sec=args.stationary_threshold_sec,
        llm_enabled=args.llm,
        llm_model=args.llm_model,
        tms_provider=args.tms_provider,
        wms_enabled=args.wms_enabled,
    )

    snapshot = result["snapshot"]
    actions = result["actions"]
    llm_metrics = result.get("llm_metrics", {})

    logging.getLogger("opssentinel.main").info(
        "run complete entities=%d exceptions=%d actions=%d correlations=%d traces=%d llm_enabled=%s tms_provider=%s wms_enabled=%s llm_calls_total=%d llm_calls_valid=%d llm_calls_invalid=%d llm_fallbacks_used=%d",
        len(snapshot["entities"]),
        len(snapshot["exceptions"]),
        len(actions),
        len(snapshot["correlations"]),
        len(snapshot["traces"]),
        result["llm_enabled"],
        result.get("tms_provider"),
        result.get("wms_enabled"),
        int(llm_metrics.get("llm_calls_total", 0)),
        int(llm_metrics.get("llm_calls_valid", 0)),
        int(llm_metrics.get("llm_calls_invalid", 0)),
        int(llm_metrics.get("llm_fallbacks_used", 0)),
    )

    print("\n--- FINAL STATE SNAPSHOT ---")
    print(json.dumps(snapshot, indent=2))
    print("\n--- EXECUTED ACTIONS ---")
    print(json.dumps(actions, indent=2))


if __name__ == "__main__":
    asyncio.run(_main(parse_args()))
