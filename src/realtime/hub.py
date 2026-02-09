import asyncio
from typing import Any, Optional


class BroadcastHub:
    def __init__(self) -> None:
        self._connections: set[Any] = set()
        self._lock = asyncio.Lock()
        self._config: Optional[dict] = None

    async def connect(self, websocket: Any) -> None:
        await websocket.accept()
        async with self._lock:
            self._connections.add(websocket)
        if self._config:
            try:
                await websocket.send_json(self._config)
            except Exception:
                await self.disconnect(websocket)

    async def disconnect(self, websocket: Any) -> None:
        async with self._lock:
            self._connections.discard(websocket)

    def set_config(self, payload: dict) -> None:
        self._config = payload

    async def broadcast(self, payload: dict) -> None:
        async with self._lock:
            targets = list(self._connections)
        for websocket in targets:
            try:
                await websocket.send_json(payload)
            except Exception:
                await self.disconnect(websocket)
