from __future__ import annotations

import asyncio
import copy
import logging
from collections.abc import Awaitable, Callable
from typing import Any, Dict

MCPToolHandler = Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]]


class MCPToolError(RuntimeError):
    pass


class MCPGateway:
    """Async MCP-style tool gateway for agent API interactions."""

    def __init__(self) -> None:
        self._tools: Dict[str, MCPToolHandler] = {}
        self._logger = logging.getLogger("opssentinel.mcp.gateway")

    def register_tool(self, name: str, handler: MCPToolHandler) -> None:
        tool_name = name.strip()
        if not tool_name:
            raise ValueError("tool name cannot be empty")
        self._tools[tool_name] = handler
        self._logger.info("registered mcp tool name=%s", tool_name)

    async def call_tool(self, name: str, arguments: Dict[str, Any], timeout_s: float = 2.0) -> Dict[str, Any]:
        handler = self._tools.get(name)
        if handler is None:
            raise MCPToolError(f"unknown MCP tool: {name}")

        safe_args = copy.deepcopy(arguments)
        self._logger.info("mcp call tool=%s args=%s", name, safe_args)

        try:
            result = await asyncio.wait_for(handler(safe_args), timeout=timeout_s)
        except TimeoutError as exc:
            raise MCPToolError(f"MCP tool timed out: {name}") from exc
        except Exception as exc:  # noqa: BLE001
            raise MCPToolError(f"MCP tool failure: {name}: {exc}") from exc

        if not isinstance(result, dict):
            raise MCPToolError(f"invalid MCP tool response for {name}: expected dict")

        self._logger.info("mcp result tool=%s result=%s", name, result)
        return copy.deepcopy(result)
