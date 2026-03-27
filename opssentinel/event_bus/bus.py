from __future__ import annotations

import asyncio
import copy
import logging
from collections import defaultdict
from typing import Any, Awaitable, Callable, Dict, List

Handler = Callable[[Dict[str, Any]], Awaitable[None]]


class EventBus:
    """Local async pub/sub bus with topic fan-out (Kafka-ready abstraction)."""

    def __init__(self) -> None:
        self._subscriber_queues: Dict[str, List[asyncio.Queue[Dict[str, Any]]]] = defaultdict(list)
        self._subscriber_tasks: List[asyncio.Task[Any]] = []
        self._logger = logging.getLogger("opssentinel.event_bus")
        self._closed = False

    async def publish(self, topic: str, message: Dict[str, Any]) -> None:
        if self._closed:
            return
        queues = self._subscriber_queues.get(topic, [])
        self._logger.info("publish topic=%s subscribers=%d event_id=%s", topic, len(queues), message.get("event_id"))
        for queue in queues:
            await queue.put(copy.deepcopy(message))

    def subscribe(self, topic: str, handler: Handler) -> None:
        if self._closed:
            raise RuntimeError("event bus is closed")

        queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue()
        self._subscriber_queues[topic].append(queue)

        async def worker() -> None:
            self._logger.info("subscribe topic=%s handler=%s", topic, getattr(handler, "__name__", "anonymous"))
            while True:
                message = await queue.get()
                try:
                    await handler(message)
                except Exception:  # noqa: BLE001
                    self._logger.exception("handler error topic=%s", topic)
                finally:
                    queue.task_done()

        task = asyncio.create_task(worker(), name=f"bus-{topic}-{len(self._subscriber_tasks)}")
        self._subscriber_tasks.append(task)

    async def drain(self) -> None:
        waits: List[Awaitable[None]] = []
        for queues in self._subscriber_queues.values():
            for queue in queues:
                waits.append(queue.join())
        if waits:
            await asyncio.gather(*waits)

    async def shutdown(self) -> None:
        self._closed = True
        for task in self._subscriber_tasks:
            task.cancel()
        if self._subscriber_tasks:
            await asyncio.gather(*self._subscriber_tasks, return_exceptions=True)
        self._logger.info("event bus shutdown complete")
