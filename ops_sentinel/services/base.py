from __future__ import annotations

class BaseService:
    def __init__(self, name: str) -> None:
        self.name = name
        self._running = True

    async def stop(self) -> None:
        self._running = False

    @property
    def running(self) -> bool:
        return self._running

    async def run(self) -> None:
        raise NotImplementedError
