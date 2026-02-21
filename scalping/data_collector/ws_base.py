"""
Базовый WebSocket клиент с reconnect/backoff.
"""

import asyncio
import json
import time
import aiohttp

from scalping.core.logger import get_logger

log = get_logger("ws_base")

INITIAL_BACKOFF = 1.0
MAX_BACKOFF = 60.0
BACKOFF_FACTOR = 2.0


class BaseWSClient:
    """
    Базовый WebSocket клиент.
    Подклассы переопределяют on_message(data: dict).
    """

    def __init__(self, url: str, name: str, shutdown_event: asyncio.Event):
        self.url = url
        self.name = name
        self.shutdown_event = shutdown_event
        self._backoff = INITIAL_BACKOFF
        self._session: aiohttp.ClientSession | None = None
        self._ws: aiohttp.ClientWebSocketResponse | None = None

    async def on_message(self, data: dict):
        """Переопределить в подклассе."""
        raise NotImplementedError

    async def run(self):
        """Главный цикл: connect → read → reconnect."""
        self._session = aiohttp.ClientSession()
        try:
            while not self.shutdown_event.is_set():
                try:
                    await self._connect_and_read()
                except (
                    aiohttp.WSServerHandshakeError,
                    aiohttp.ClientConnectorError,
                    aiohttp.ClientError,
                    ConnectionResetError,
                    asyncio.TimeoutError,
                ) as e:
                    log.warning("[%s] WS error: %s, reconnect in %.1fs", self.name, e, self._backoff)
                except Exception as e:
                    log.exception("[%s] Unexpected WS error: %s", self.name, e)

                if self.shutdown_event.is_set():
                    break

                # Backoff
                await asyncio.sleep(self._backoff)
                self._backoff = min(self._backoff * BACKOFF_FACTOR, MAX_BACKOFF)
        finally:
            if self._session and not self._session.closed:
                await self._session.close()

    async def _connect_and_read(self):
        log.info("[%s] Connecting to %s", self.name, self.url[:80])
        async with self._session.ws_connect(
            self.url,
            heartbeat=20,
            timeout=aiohttp.ClientWSCloseCode(30),
        ) as ws:
            self._ws = ws
            self._backoff = INITIAL_BACKOFF  # reset backoff on success
            log.info("[%s] Connected", self.name)

            async for msg in ws:
                if self.shutdown_event.is_set():
                    break
                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        data = json.loads(msg.data)
                        await self.on_message(data)
                    except json.JSONDecodeError:
                        log.warning("[%s] Bad JSON: %s", self.name, msg.data[:100])
                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    log.warning("[%s] WS closed/error: %s", self.name, msg.type)
                    break

        log.info("[%s] Disconnected", self.name)
