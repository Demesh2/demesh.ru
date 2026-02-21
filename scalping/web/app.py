"""
FastAPI веб-панель мониторинга.

GET /          — HTML страница
GET /api/state — JSON со всем состоянием (polling каждую 1 сек)
"""

import time
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

from scalping.core.state import SharedState
from scalping.core.config import SYMBOLS
from scalping.paper_trading.trader import PaperTrader

app = FastAPI(title="Binance ML Scalping Monitor", version="1.0.0")

# Глобальная ссылка на state — устанавливается при запуске
_state: SharedState | None = None


def set_state(state: SharedState):
    global _state
    _state = state


@app.get("/", response_class=HTMLResponse)
async def index():
    """Отдать HTML страницу мониторинга."""
    html_path = Path(__file__).parent / "dashboard.html"
    return HTMLResponse(content=html_path.read_text(encoding="utf-8"))


@app.get("/api/state")
async def get_state():
    """JSON со всем состоянием для polling."""
    if _state is None:
        return JSONResponse({"error": "State not initialized"}, status_code=503)

    now = time.time()
    uptime = now - _state.started_at

    symbols_data = {}
    for symbol in SYMBOLS:
        ss = _state.get(symbol)
        feats = ss.features

        # Paper trading stats
        stats = PaperTrader.compute_stats(list(ss.paper_trades))

        # Последние 20 сделок
        recent_trades = []
        closed_trades = [t for t in ss.paper_trades if t.exit_ts is not None]
        for t in list(closed_trades)[-20:]:
            recent_trades.append({
                "side": t.side,
                "entry_price": round(t.entry_price, 4),
                "exit_price": round(t.exit_price, 4) if t.exit_price else None,
                "exit_reason": t.exit_reason,
                "pnl_bps": round(t.pnl_bps, 2) if t.pnl_bps is not None else None,
                "prob": round(t.prob, 3),
                "duration_s": round(t.exit_ts - t.entry_ts, 1) if t.exit_ts else None,
            })

        # Открытая позиция
        open_trade = None
        if ss.open_paper_trade:
            ot = ss.open_paper_trade
            open_trade = {
                "side": ot.side,
                "entry_price": round(ot.entry_price, 4),
                "entry_ts": round(ot.entry_ts, 1),
                "duration_s": round(now - ot.entry_ts, 1),
                "prob": round(ot.prob, 3),
            }

        symbols_data[symbol] = {
            # Market data
            "mid_price": round(feats.get("mid_price", 0), 4),
            "spread_ticks": round(feats.get("spread_ticks", 0), 2),
            "spread_bps": round(feats.get("spread_bps", 0), 2),

            # Order book
            "imbalance_top10": round(feats.get("imbalance_top10", 0), 4),
            "imbalance_top20": round(feats.get("imbalance_top20", 0), 4),
            "book_depth_ratio": round(feats.get("book_depth_ratio", 0), 3),

            # Tape
            "trade_rate_10s": round(feats.get("trade_rate_10s", 0), 2),
            "signed_vol_10s": round(feats.get("signed_vol_10s", 0), 2),
            "signed_vol_30s": round(feats.get("signed_vol_30s", 0), 2),
            "cvd_60s": round(feats.get("cvd_60s", 0), 2),
            "cvd_slope": round(feats.get("cvd_slope", 0), 2),

            # Spot
            "basis_bps": round(feats.get("basis_bps", 0), 3),
            "spot_confirm_score": round(feats.get("spot_confirm_score", 0), 1),

            # Volatility
            "atr_1m_14": round(feats.get("atr_1m_14", 0), 4),
            "atr_bps": round(feats.get("atr_bps", 0), 2),

            # Inference
            "prob_tp_first": round(ss.prob_tp_first, 4),
            "gate_allowed": ss.gate_allowed,
            "gate_reasons": ss.gate_reasons,

            # Health
            "ws_lag_p95": round(ss.health.ws_lag_p95(), 1),
            "depth_sync_ok": ss.health.depth_sync_ok,
            "resync_count_10m": ss.health.resync_count_10m,

            # Paper trading
            "paper_stats": stats,
            "recent_trades": recent_trades,
            "open_trade": open_trade,
        }

    return {
        "timestamp": now,
        "uptime_s": round(uptime, 1),
        "model_loaded": _state.model_loaded,
        "symbols": symbols_data,
    }
