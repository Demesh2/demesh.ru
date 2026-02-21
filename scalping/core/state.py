"""
Shared state — потокобезопасное (asyncio) хранилище данных
между коллекторами, feature engine, inference и веб-панелью.

Всё в одном asyncio event loop, поэтому asyncio.Lock достаточно.
"""

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Deque

from scalping.core.config import SYMBOLS, PAPER_MAX_TRADES_MEM


@dataclass
class OrderBook:
    """Локальный order book для одного символа."""
    bids: Dict[float, float] = field(default_factory=dict)  # price -> qty
    asks: Dict[float, float] = field(default_factory=dict)
    last_update_id: int = 0
    synced: bool = False
    resync_count: int = 0  # суммарно
    last_resync_ts: float = 0.0

    def best_bid(self) -> Optional[float]:
        return max(self.bids.keys()) if self.bids else None

    def best_ask(self) -> Optional[float]:
        return min(self.asks.keys()) if self.asks else None

    def mid_price(self) -> Optional[float]:
        bb, ba = self.best_bid(), self.best_ask()
        if bb is not None and ba is not None:
            return (bb + ba) / 2.0
        return None

    def spread(self) -> Optional[float]:
        bb, ba = self.best_bid(), self.best_ask()
        if bb is not None and ba is not None:
            return ba - bb
        return None


@dataclass
class TradeRecord:
    """Один агрегированный трейд."""
    ts: float         # timestamp в секундах
    price: float
    qty: float
    is_buyer_maker: bool  # True = продажа (sell), False = покупка (buy)


@dataclass
class Kline:
    """Одна свеча 1m."""
    open_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    close_time: int
    is_closed: bool


@dataclass
class BookTicker:
    """Best bid/ask из bookTicker стрима."""
    bid_price: float = 0.0
    bid_qty: float = 0.0
    ask_price: float = 0.0
    ask_qty: float = 0.0
    ts: float = 0.0


@dataclass
class PaperTrade:
    """Одна paper-сделка."""
    symbol: str
    side: str  # "LONG" / "SHORT"
    entry_price: float
    entry_ts: float
    exit_price: Optional[float] = None
    exit_ts: Optional[float] = None
    exit_reason: Optional[str] = None  # "TP" / "SL" / "TIMEOUT"
    pnl_bps: Optional[float] = None
    prob: float = 0.0


@dataclass
class HealthMetrics:
    """Метрики здоровья WS-соединений для одного символа."""
    ws_lag_samples: Deque[float] = field(default_factory=lambda: deque(maxlen=200))
    depth_sync_ok: bool = False
    resync_count_10m: int = 0
    _resync_times: Deque[float] = field(default_factory=lambda: deque(maxlen=100))

    def record_lag(self, lag_ms: float):
        self.ws_lag_samples.append(lag_ms)

    def ws_lag_p95(self) -> float:
        if not self.ws_lag_samples:
            return 0.0
        sorted_lags = sorted(self.ws_lag_samples)
        idx = int(len(sorted_lags) * 0.95)
        return sorted_lags[min(idx, len(sorted_lags) - 1)]

    def record_resync(self):
        now = time.time()
        self._resync_times.append(now)
        cutoff = now - 600  # 10 минут
        while self._resync_times and self._resync_times[0] < cutoff:
            self._resync_times.popleft()
        self.resync_count_10m = len(self._resync_times)


@dataclass
class SymbolState:
    """Всё состояние для одного символа."""
    symbol: str

    # Order books
    futures_book: OrderBook = field(default_factory=OrderBook)
    spot_book_ticker: BookTicker = field(default_factory=BookTicker)

    # Trades (deque с лимитом ~60 секунд данных, обрезается в коллекторе)
    futures_trades: Deque[TradeRecord] = field(default_factory=lambda: deque(maxlen=50000))
    spot_trades: Deque[TradeRecord] = field(default_factory=lambda: deque(maxlen=10000))

    # Klines 1m (последние 20 штук)
    futures_klines: Deque[Kline] = field(default_factory=lambda: deque(maxlen=30))
    spot_klines: Deque[Kline] = field(default_factory=lambda: deque(maxlen=30))

    # Book ticker (futures)
    futures_book_ticker: BookTicker = field(default_factory=BookTicker)

    # Health
    health: HealthMetrics = field(default_factory=HealthMetrics)

    # Последние вычисленные фичи (dict feature_name -> value)
    features: Dict[str, float] = field(default_factory=dict)

    # Live inference
    prob_tp_first: float = 0.0
    gate_allowed: bool = False
    gate_reasons: List[str] = field(default_factory=list)

    # Paper trades
    paper_trades: Deque[PaperTrade] = field(default_factory=lambda: deque(maxlen=PAPER_MAX_TRADES_MEM))
    open_paper_trade: Optional[PaperTrade] = None

    # Depth buffer для начальной синхронизации
    _depth_buffer: List = field(default_factory=list)
    _depth_buffer_active: bool = True


class SharedState:
    """
    Глобальное состояние всего приложения.
    Доступ через asyncio.Lock для безопасности.
    """

    def __init__(self):
        self.lock = asyncio.Lock()
        self.symbols: Dict[str, SymbolState] = {
            s: SymbolState(symbol=s) for s in SYMBOLS
        }
        self.started_at = time.time()
        self.shutdown_event = asyncio.Event()
        self.model_loaded = False
        self.model_path: Optional[str] = None

    def get(self, symbol: str) -> SymbolState:
        return self.symbols[symbol]
