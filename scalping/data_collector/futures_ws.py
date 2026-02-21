"""
Futures WebSocket коллекторы:
- bookTicker
- aggTrades
- kline_1m
"""

import time

from scalping.core.logger import get_logger
from scalping.core.state import SharedState, TradeRecord, Kline, BookTicker
from scalping.core.config import FUTURES_WS_BASE
from scalping.data_collector.ws_base import BaseWSClient

log = get_logger("futures_ws")


class FuturesBookTickerWS(BaseWSClient):
    """Futures bookTicker stream."""

    def __init__(self, symbol: str, state: SharedState):
        sym_lower = symbol.lower()
        url = f"{FUTURES_WS_BASE}/ws/{sym_lower}@bookTicker"
        super().__init__(url, f"fut_bt_{symbol}", state.shutdown_event)
        self.symbol = symbol
        self.state = state

    async def on_message(self, data: dict):
        now = time.time()
        ss = self.state.get(self.symbol)

        event_time_ms = data.get("E", 0) or data.get("T", 0)
        if event_time_ms:
            lag = now * 1000 - event_time_ms
            ss.health.record_lag(lag)

        ss.futures_book_ticker = BookTicker(
            bid_price=float(data.get("b", 0)),
            bid_qty=float(data.get("B", 0)),
            ask_price=float(data.get("a", 0)),
            ask_qty=float(data.get("A", 0)),
            ts=now,
        )


class FuturesAggTradesWS(BaseWSClient):
    """Futures aggTrades stream."""

    def __init__(self, symbol: str, state: SharedState):
        sym_lower = symbol.lower()
        url = f"{FUTURES_WS_BASE}/ws/{sym_lower}@aggTrade"
        super().__init__(url, f"fut_at_{symbol}", state.shutdown_event)
        self.symbol = symbol
        self.state = state

    async def on_message(self, data: dict):
        now = time.time()
        ss = self.state.get(self.symbol)

        event_time_ms = data.get("E", 0)
        if event_time_ms:
            lag = now * 1000 - event_time_ms
            ss.health.record_lag(lag)

        trade = TradeRecord(
            ts=data.get("T", data.get("E", now * 1000)) / 1000.0,
            price=float(data["p"]),
            qty=float(data["q"]),
            is_buyer_maker=data.get("m", False),
        )
        ss.futures_trades.append(trade)

        # Обрезка: оставляем только последние 120 секунд
        cutoff = now - 120
        while ss.futures_trades and ss.futures_trades[0].ts < cutoff:
            ss.futures_trades.popleft()


class FuturesKlineWS(BaseWSClient):
    """Futures kline_1m stream."""

    def __init__(self, symbol: str, state: SharedState):
        sym_lower = symbol.lower()
        url = f"{FUTURES_WS_BASE}/ws/{sym_lower}@kline_1m"
        super().__init__(url, f"fut_kl_{symbol}", state.shutdown_event)
        self.symbol = symbol
        self.state = state

    async def on_message(self, data: dict):
        k = data.get("k", {})
        if not k:
            return

        ss = self.state.get(self.symbol)
        kline = Kline(
            open_time=k["t"],
            open=float(k["o"]),
            high=float(k["h"]),
            low=float(k["l"]),
            close=float(k["c"]),
            volume=float(k["v"]),
            close_time=k["T"],
            is_closed=k.get("x", False),
        )

        # Обновляем последнюю свечу если она та же, или добавляем новую
        if ss.futures_klines and ss.futures_klines[-1].open_time == kline.open_time:
            ss.futures_klines[-1] = kline
        else:
            ss.futures_klines.append(kline)
