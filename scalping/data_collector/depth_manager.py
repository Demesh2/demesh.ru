"""
Менеджер order book с корректной синхронизацией depth@250ms.

Алгоритм (по документации Binance):
  a) Открыть diff stream depth@250ms и буферизовать события.
  b) Получить snapshot через REST /fapi/v1/depth.
  c) Отбросить buffered события, где u < lastUpdateId + 1.
  d) Применить buffered события, начиная с первого где U <= lastUpdateId + 1 <= u.
  e) Далее каждое новое diff событие применять только если pu == prev_u.
  f) При пропуске/ошибке — resync.
"""

import asyncio
import json
import time

import aiohttp

from scalping.core.logger import get_logger
from scalping.core.state import SharedState, OrderBook
from scalping.core.config import (
    FUTURES_WS_BASE,
    FUTURES_REST_BASE,
    DEPTH_SNAPSHOT_LIMIT,
)

log = get_logger("depth_mgr")


class DepthManager:
    """
    Управляет order book для одного символа.
    Запускается как asyncio task.
    """

    def __init__(self, symbol: str, state: SharedState):
        self.symbol = symbol
        self.state = state
        self.shutdown = state.shutdown_event
        self._prev_final_update_id: int = 0

    async def run(self):
        """Основной цикл: подключение к diff stream + sync."""
        while not self.shutdown.is_set():
            try:
                await self._run_stream()
            except Exception as e:
                log.exception("[depth_%s] Error: %s", self.symbol, e)
            if not self.shutdown.is_set():
                ss = self.state.get(self.symbol)
                ss.futures_book.synced = False
                ss.health.depth_sync_ok = False
                await asyncio.sleep(2)

    async def _run_stream(self):
        sym_lower = self.symbol.lower()
        url = f"{FUTURES_WS_BASE}/ws/{sym_lower}@depth@250ms"
        ss = self.state.get(self.symbol)

        # Сброс состояния
        ss.futures_book = OrderBook()
        ss.futures_book.synced = False
        ss.health.depth_sync_ok = False
        ss._depth_buffer = []
        ss._depth_buffer_active = True

        async with aiohttp.ClientSession() as session:
            log.info("[depth_%s] Connecting to diff stream...", self.symbol)
            async with session.ws_connect(url, heartbeat=20) as ws:
                log.info("[depth_%s] Diff stream connected, fetching snapshot...", self.symbol)

                # Запускаем чтение diff events в буфер
                buffer_task = asyncio.create_task(
                    self._buffer_events(ws, ss)
                )

                # Получаем REST snapshot
                snapshot = await self._get_snapshot(session)
                if snapshot is None:
                    buffer_task.cancel()
                    return

                last_update_id = snapshot["lastUpdateId"]
                log.info(
                    "[depth_%s] Snapshot received, lastUpdateId=%d, bids=%d, asks=%d",
                    self.symbol, last_update_id, len(snapshot["bids"]), len(snapshot["asks"]),
                )

                # Применяем snapshot
                book = ss.futures_book
                book.bids = {float(p): float(q) for p, q in snapshot["bids"]}
                book.asks = {float(p): float(q) for p, q in snapshot["asks"]}
                book.last_update_id = last_update_id

                # Останавливаем буферизацию
                ss._depth_buffer_active = False
                await asyncio.sleep(0.1)  # дать buffer_task записать последнее

                # Обрабатываем буфер
                buffered = list(ss._depth_buffer)
                ss._depth_buffer = []
                applied = 0

                for event in buffered:
                    u_final = event["u"]   # Final update ID
                    u_first = event["U"]   # First update ID
                    pu = event.get("pu", 0)

                    # Шаг c: отбросить где u < lastUpdateId + 1
                    if u_final < last_update_id + 1:
                        continue

                    # Шаг d: первое применимое событие
                    if not book.synced:
                        if u_first <= last_update_id + 1 <= u_final:
                            self._apply_depth_update(book, event)
                            self._prev_final_update_id = u_final
                            book.synced = True
                            ss.health.depth_sync_ok = True
                            applied += 1
                        continue

                    # Шаг e: непрерывность
                    if pu == self._prev_final_update_id:
                        self._apply_depth_update(book, event)
                        self._prev_final_update_id = u_final
                        applied += 1
                    else:
                        log.warning(
                            "[depth_%s] Discontinuity in buffer: pu=%d, prev=%d. Resync.",
                            self.symbol, pu, self._prev_final_update_id,
                        )
                        ss.health.record_resync()
                        book.resync_count += 1
                        buffer_task.cancel()
                        return

                log.info("[depth_%s] Buffer applied: %d events, synced=%s", self.symbol, applied, book.synced)

                if not book.synced:
                    log.warning("[depth_%s] Could not sync from buffer, resync.", self.symbol)
                    ss.health.record_resync()
                    book.resync_count += 1
                    buffer_task.cancel()
                    return

                # Отменяем buffer task и переходим к прямому чтению
                buffer_task.cancel()
                try:
                    await buffer_task
                except asyncio.CancelledError:
                    pass

                # Основной цикл чтения diff событий
                async for msg in ws:
                    if self.shutdown.is_set():
                        break
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        event = json.loads(msg.data)
                        u_final = event["u"]
                        pu = event.get("pu", 0)

                        # Проверка непрерывности
                        if pu != self._prev_final_update_id:
                            log.warning(
                                "[depth_%s] Discontinuity: pu=%d, prev=%d. Resync.",
                                self.symbol, pu, self._prev_final_update_id,
                            )
                            ss.health.record_resync()
                            ss.futures_book.synced = False
                            ss.health.depth_sync_ok = False
                            book.resync_count += 1
                            return

                        self._apply_depth_update(book, event)
                        self._prev_final_update_id = u_final

                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        log.warning("[depth_%s] WS closed", self.symbol)
                        break

    async def _buffer_events(self, ws, ss):
        """Буферизует diff события до получения snapshot."""
        try:
            async for msg in ws:
                if self.shutdown.is_set():
                    break
                if not ss._depth_buffer_active:
                    break
                if msg.type == aiohttp.WSMsgType.TEXT:
                    event = json.loads(msg.data)
                    ss._depth_buffer.append(event)
                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    break
        except asyncio.CancelledError:
            pass

    async def _get_snapshot(self, session: aiohttp.ClientSession) -> dict | None:
        """Получить REST snapshot с ретраями."""
        url = f"{FUTURES_REST_BASE}/fapi/v1/depth"
        params = {"symbol": self.symbol, "limit": DEPTH_SNAPSHOT_LIMIT}

        for attempt in range(4):
            try:
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    log.warning(
                        "[depth_%s] Snapshot HTTP %d, attempt %d",
                        self.symbol, resp.status, attempt + 1,
                    )
            except Exception as e:
                log.warning("[depth_%s] Snapshot error: %s, attempt %d", self.symbol, e, attempt + 1)

            await asyncio.sleep(2 ** attempt)

        log.error("[depth_%s] Failed to get snapshot after 4 attempts", self.symbol)
        return None

    @staticmethod
    def _apply_depth_update(book: OrderBook, event: dict):
        """Применить diff update к book."""
        for price_str, qty_str in event.get("b", []):
            price = float(price_str)
            qty = float(qty_str)
            if qty == 0.0:
                book.bids.pop(price, None)
            else:
                book.bids[price] = qty

        for price_str, qty_str in event.get("a", []):
            price = float(price_str)
            qty = float(qty_str)
            if qty == 0.0:
                book.asks.pop(price, None)
            else:
                book.asks[price] = qty

        book.last_update_id = event["u"]
