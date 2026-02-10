import asyncio
import logging
from typing import Any, Optional


class BroadcastHub:
    def __init__(self, logger: Optional[logging.Logger] = None) -> None:
        self._connections: set[Any] = set()
        self._lock = asyncio.Lock()
        self._config: Optional[dict] = None
        self._logger = logger or logging.getLogger("market_microstructure")
        self._last_no_connection_log = 0.0

    async def connect(self, websocket: Any) -> None:
        await websocket.accept()
        async with self._lock:
            self._connections.add(websocket)
        self._logger.info("✅ WebSocket 前端已连接，当前前端连接数: %s", len(self._connections))
        if self._config:
            try:
                await websocket.send_json(self._config)
            except Exception:
                await self.disconnect(websocket)

    async def disconnect(self, websocket: Any) -> None:
        async with self._lock:
            self._connections.discard(websocket)
        self._logger.info("ℹ️ WebSocket 前端断开，当前前端连接数: %s", len(self._connections))

    def set_config(self, payload: dict) -> None:
        self._config = payload

    async def broadcast(self, payload: dict) -> None:
        async with self._lock:
            targets = list(self._connections)
        if not targets:
            now = asyncio.get_event_loop().time()
            if now - self._last_no_connection_log > 30:
                self._logger.warning("⚠️ WebSocket 无前端连接，未推送实时数据")
                self._last_no_connection_log = now
        for websocket in targets:
            try:
                await websocket.send_json(payload)
            except Exception:
                await self.disconnect(websocket)
