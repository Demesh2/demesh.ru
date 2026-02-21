"""
Paper Trading — виртуальная торговля на основе сигналов.

Вход: по ask (для LONG) — консервативный подход (worst-case fill).
Выход: по TP/SL барьерам.
"""

import asyncio
import time
from datetime import datetime, timezone

import pandas as pd

from scalping.core.logger import get_logger
from scalping.core.state import SharedState, PaperTrade, SymbolState
from scalping.core.config import (
    SYMBOLS,
    SYMBOL_CONFIGS,
    LABEL_HORIZON_SEC,
    PAPER_FEE_BPS,
    PAPER_SLIPPAGE_BPS,
    REPORTS_DIR,
)
from scalping.labeling.labeler import compute_barriers

log = get_logger("paper_trader")


class PaperTrader:
    """Paper trading: вход/выход, учёт PnL."""

    def __init__(self, state: SharedState):
        self.state = state
        self._flush_interval = 600  # сброс отчёта каждые 10 минут
        self._last_flush = time.time()

    async def run(self):
        log.info("Paper trader started")
        shutdown = self.state.shutdown_event

        while not shutdown.is_set():
            now = time.time()

            for symbol in SYMBOLS:
                try:
                    ss = self.state.get(symbol)
                    self._process_symbol(ss, now)
                except Exception as e:
                    log.exception("Paper trading error for %s: %s", symbol, e)

            # Периодический flush
            if now - self._last_flush > self._flush_interval:
                self._flush_reports()

            try:
                await asyncio.wait_for(shutdown.wait(), timeout=1.0)
                break
            except asyncio.TimeoutError:
                pass

        self._flush_reports()
        log.info("Paper trader stopped")

    def _process_symbol(self, ss: SymbolState, now: float):
        """Обработка одного символа."""
        cfg = SYMBOL_CONFIGS.get(ss.symbol)
        if cfg is None:
            return

        feats = ss.features
        mid = feats.get("mid_price", 0)
        atr = feats.get("atr_1m_14", 0)

        if mid <= 0 or atr <= 0:
            return

        # Проверяем открытую позицию
        if ss.open_paper_trade is not None:
            self._check_exit(ss, mid, atr, now)
            return

        # Проверяем вход: gate_allowed
        if ss.gate_allowed and self.state.model_loaded:
            self._enter_trade(ss, mid, atr, now)

    def _enter_trade(self, ss: SymbolState, mid: float, atr: float, now: float):
        """Открыть paper-позицию."""
        # Вход по ask (worst case для LONG)
        ask = ss.futures_book_ticker.ask_price
        if ask <= 0:
            ask = mid  # fallback

        # Slippage: добавляем buffer
        slippage = ask * (PAPER_SLIPPAGE_BPS / 10000)
        entry_price = ask + slippage

        trade = PaperTrade(
            symbol=ss.symbol,
            side="LONG",
            entry_price=entry_price,
            entry_ts=now,
            prob=ss.prob_tp_first,
        )
        ss.open_paper_trade = trade

        log.info(
            "[%s] PAPER ENTRY: LONG @ %.4f (ask=%.4f, prob=%.3f)",
            ss.symbol, entry_price, ask, ss.prob_tp_first,
        )

    def _check_exit(self, ss: SymbolState, mid: float, atr: float, now: float):
        """Проверить выход из позиции."""
        trade = ss.open_paper_trade
        if trade is None:
            return

        d_eff = compute_barriers(atr, ss.symbol)
        tp_price = trade.entry_price + d_eff
        sl_price = trade.entry_price - d_eff
        timeout = trade.entry_ts + LABEL_HORIZON_SEC

        exit_reason = None
        exit_price = None

        if mid >= tp_price:
            exit_reason = "TP"
            # Выход по bid (worst case)
            bid = ss.futures_book_ticker.bid_price
            exit_price = bid if bid > 0 else mid
        elif mid <= sl_price:
            exit_reason = "SL"
            bid = ss.futures_book_ticker.bid_price
            exit_price = bid if bid > 0 else mid
        elif now >= timeout:
            exit_reason = "TIMEOUT"
            bid = ss.futures_book_ticker.bid_price
            exit_price = bid if bid > 0 else mid

        if exit_reason is not None:
            # Считаем PnL
            gross_pnl_bps = ((exit_price - trade.entry_price) / trade.entry_price) * 10000
            fees = PAPER_FEE_BPS  # round-trip fees
            net_pnl_bps = gross_pnl_bps - fees

            trade.exit_price = exit_price
            trade.exit_ts = now
            trade.exit_reason = exit_reason
            trade.pnl_bps = net_pnl_bps

            ss.paper_trades.append(trade)
            ss.open_paper_trade = None

            log.info(
                "[%s] PAPER EXIT: %s @ %.4f, PnL=%.1f bps (gross=%.1f, fees=%.1f)",
                ss.symbol, exit_reason, exit_price,
                net_pnl_bps, gross_pnl_bps, fees,
            )

    def _flush_reports(self):
        """Сохранить отчёт paper trades в parquet."""
        for symbol in SYMBOLS:
            ss = self.state.get(symbol)
            trades = [t for t in ss.paper_trades if t.exit_ts is not None]
            if not trades:
                continue

            rows = []
            for t in trades:
                rows.append({
                    "symbol": t.symbol,
                    "side": t.side,
                    "entry_price": t.entry_price,
                    "entry_ts": t.entry_ts,
                    "exit_price": t.exit_price,
                    "exit_ts": t.exit_ts,
                    "exit_reason": t.exit_reason,
                    "pnl_bps": t.pnl_bps,
                    "prob": t.prob,
                })

            df = pd.DataFrame(rows)
            date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
            path = REPORTS_DIR / f"papertrades_{symbol}_{date_str}.parquet"

            if path.exists():
                existing = pd.read_parquet(path)
                df = pd.concat([existing, df], ignore_index=True)
                df = df.drop_duplicates(subset=["entry_ts", "symbol"])

            df.to_parquet(path, index=False)
            log.info("Paper trades report saved: %s (%d trades)", path.name, len(df))

        self._last_flush = time.time()

    @staticmethod
    def compute_stats(trades) -> dict:
        """Посчитать статистику по списку сделок."""
        closed = [t for t in trades if t.exit_ts is not None]
        if not closed:
            return {
                "total": 0,
                "wins": 0,
                "losses": 0,
                "win_rate": 0.0,
                "avg_pnl_bps": 0.0,
                "total_pnl_bps": 0.0,
                "max_dd_bps": 0.0,
            }

        pnls = [t.pnl_bps for t in closed]
        wins = sum(1 for p in pnls if p > 0)
        losses = sum(1 for p in pnls if p <= 0)

        # Max drawdown (от пика equity)
        equity = 0.0
        peak = 0.0
        max_dd = 0.0
        for p in pnls:
            equity += p
            peak = max(peak, equity)
            dd = peak - equity
            max_dd = max(max_dd, dd)

        return {
            "total": len(closed),
            "wins": wins,
            "losses": losses,
            "win_rate": wins / len(closed) * 100 if closed else 0.0,
            "avg_pnl_bps": sum(pnls) / len(pnls) if pnls else 0.0,
            "total_pnl_bps": sum(pnls),
            "max_dd_bps": max_dd,
            "equity_curve": list(_cumsum(pnls)),
        }


def _cumsum(values):
    s = 0.0
    for v in values:
        s += v
        yield s
