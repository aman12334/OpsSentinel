from __future__ import annotations

import argparse
import asyncio
import json

from ops_sentinel.runtime import OpsSentinelRuntime


async def _main(max_events: int, interval_s: float, grace_s: float) -> None:
    runtime = OpsSentinelRuntime(max_events=max_events, produce_interval_s=interval_s)
    summary = await runtime.run(grace_period_s=grace_s)
    print(json.dumps(
        {
            "topic_sizes": summary.topic_sizes,
            "offsets": summary.offsets,
            "entities_tracked": len(summary.state["entity_state"]),
            "exceptions_total": len(summary.state["exceptions"]),
            "actions_total": len(summary.state["actions"]),
            "audit_events": len(summary.state["audit_log"]),
        },
        indent=2,
    ))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run OpsSentinel microservice simulation")
    parser.add_argument("--max-events", type=int, default=40)
    parser.add_argument("--interval-s", type=float, default=0.15)
    parser.add_argument("--grace-s", type=float, default=1.0)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(_main(args.max_events, args.interval_s, args.grace_s))
