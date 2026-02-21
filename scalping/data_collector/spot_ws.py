"""
Spot WebSocket коллекторы:
- bookTicker
- aggTrades
- kline_1m
"""

import time

from scalping.core.logger import get_logger
from scalping.core.state import SharedState, TradeRecord, Kline, BookTicker
from scalping.core.config import SPOT_WS_BASE
from scalping.data_collector.ws_base import BaseWSClient

log = get_logger("spot_ws")


class SpotBookTickerWS(BaseWSClient):
    """Spot bookTicker stream."""

    def __init__(self, symbol: str, state: SharedState):
        sym_lower = symbol.lower()
        url = f"{SPOT_WS_BASE}/ws/{sym_lower}@bookTicker"
        super().__init__(url, f"spot_bt_{symbol}", state.shutdown_event)
        self.symbol = symbol
        self.state = state

    async def on_message(self, data: dict):
        now = time.time()
        ss = self.state.get(self.symbol)
        ss.spot_book_ticker = BookTicker(
            bid_price=float(data.get("b", 0)),
            bid_qty=float(data.get("B", 0)),
            ask_price=float(data.get("a", 0)),
            ask_qty=float(data.get("A", 0)),
            ts=now,
        )


class SpotAggTradesWS(BaseWSClient):
    """Spot aggTrades stream."""

    def __init__(self, symbol: str, state: SharedState):
        sym_lower = symbol.lower()
        url = f"{SPOT_WS_BASE}/ws/{sym_lower}@aggTrade"
        super().__init__(url, f"spot_at_{symbol}", state.shutdown_event)
        self.symbol = symbol
        self.state = state

    async def on_message(self, data: dict):
        now = time.time()
        ss = self.state.get(self.symbol)

        trade = TradeRecord(
            ts=data.get("T", data.get("E", now * 1000)) / 1000.0,
            price=float(data["p"]),
            qty=float(data["q"]),
            is_buyer_maker=data.get("m", False),
        )
        ss.spot_trades.append(trade)

        # Обрезка — 120 секунд
        cutoff = now - 120
        while ss.spot_trades and ss.spot_trades[0].ts < cutoff:
            ss.spot_trades.popleft()


class SpotKlineWS(BaseWSClient):
    """Spot kline_1m stream."""

    def __init__(self, symbol: str, state: SharedState):
        sym_lower = symbol.lower()
        url = f"{SPOT_WS_BASE}/ws/{sym_lower}@kline_1m"
        super().__init__(url, f"spot_kl_{symbol}", state.shutdown_event)
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

        if ss.spot_klines and ss.spot_klines[-1].open_time == kline.open_time:
            ss.spot_klines[-1] = kline
        else:
            ss.spot_klines.append(kline)
