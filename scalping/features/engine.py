"""
Feature Engine — вычисляет 40–80 фич каждую секунду для каждого символа.

Группы фич:
1. Tape (Futures) — trade_rate, signed_vol, CVD, burstiness, entropy, switch_rate, trade_size
2. Order Book (Futures) — spread, imbalance, liquidity, vacuum, walls, replenish
3. Spot confirm — basis, basis_z, spot_signed_vol, confirm_score
4. Volatility — ATR_1m(14)

Все фичи строго из данных на момент t (без будущего).
"""

import math
import time
from collections import deque
from typing import Dict, List, Optional

import numpy as np

from scalping.core.logger import get_logger
from scalping.core.state import SymbolState, TradeRecord, Kline
from scalping.core.config import SYMBOL_CONFIGS

log = get_logger("features")


def compute_features(ss: SymbolState, now: float) -> Dict[str, float]:
    """
    Вычислить все фичи для символа на момент now.
    Возвращает словарь {feature_name: value}.
    """
    feats: Dict[str, float] = {}

    # ─── Tape features (Futures) ──────────────────────────────────
    _tape_features(ss, now, feats)

    # ─── Order Book features ──────────────────────────────────────
    _orderbook_features(ss, now, feats)

    # ─── Spot confirm ─────────────────────────────────────────────
    _spot_features(ss, now, feats)

    # ─── Volatility ───────────────────────────────────────────────
    _volatility_features(ss, now, feats)

    return feats


# ═══════════════════════════════════════════════════════════════════
# Tape features (Futures trades)
# ═══════════════════════════════════════════════════════════════════

def _tape_features(ss: SymbolState, now: float, feats: Dict[str, float]):
    trades = list(ss.futures_trades)
    if not trades:
        _fill_tape_defaults(feats)
        return

    # Трейды за разные окна
    t1 = [t for t in trades if t.ts >= now - 1]
    t10 = [t for t in trades if t.ts >= now - 10]
    t30 = [t for t in trades if t.ts >= now - 30]
    t60 = [t for t in trades if t.ts >= now - 60]

    # trade_rate (трейдов/сек)
    feats["trade_rate_1s"] = float(len(t1))
    feats["trade_rate_10s"] = len(t10) / 10.0 if t10 else 0.0
    feats["trade_rate_30s"] = len(t30) / 30.0 if t30 else 0.0

    # signed_vol: buy_vol - sell_vol (buy = not is_buyer_maker)
    def signed_vol(tlist):
        buy = sum(t.qty * t.price for t in tlist if not t.is_buyer_maker)
        sell = sum(t.qty * t.price for t in tlist if t.is_buyer_maker)
        return buy - sell

    feats["signed_vol_10s"] = signed_vol(t10)
    feats["signed_vol_30s"] = signed_vol(t30)

    # CVD 60s (cumulative volume delta) + slope
    cvd_60 = signed_vol(t60)
    feats["cvd_60s"] = cvd_60

    # CVD slope: линейная регрессия CVD по 10-секундным окнам
    if len(t60) > 10:
        # Простой slope: CVD за последние 30s минус CVD за 30-60s назад
        t30_60 = [t for t in trades if now - 60 <= t.ts < now - 30]
        cvd_first_half = signed_vol(t30_60)
        cvd_second_half = signed_vol(t30)
        feats["cvd_slope"] = cvd_second_half - cvd_first_half
    else:
        feats["cvd_slope"] = 0.0

    # Burstiness / CV of inter-trade intervals
    if len(t10) >= 3:
        dts = [t10[i].ts - t10[i - 1].ts for i in range(1, len(t10))]
        mean_dt = np.mean(dts)
        std_dt = np.std(dts)
        feats["burstiness_10s"] = std_dt / mean_dt if mean_dt > 0 else 0.0
    else:
        feats["burstiness_10s"] = 0.0

    # Entropy (упрощённый "нервный поток" через 100ms bins)
    if t10:
        n_bins = 100  # 10 секунд / 100ms
        bins = [0] * n_bins
        for t in t10:
            idx = int((t.ts - (now - 10)) * 10)  # 10 bins per second
            idx = max(0, min(idx, n_bins - 1))
            bins[idx] += 1
        total = sum(bins)
        if total > 0:
            probs = [b / total for b in bins if b > 0]
            feats["entropy_10s"] = -sum(p * math.log2(p) for p in probs)
        else:
            feats["entropy_10s"] = 0.0
    else:
        feats["entropy_10s"] = 0.0

    # switch_rate: buy↔sell переключений за 10s
    if len(t10) >= 2:
        switches = sum(
            1 for i in range(1, len(t10))
            if t10[i].is_buyer_maker != t10[i - 1].is_buyer_maker
        )
        feats["switch_rate_10s"] = switches / len(t10)
    else:
        feats["switch_rate_10s"] = 0.0

    # Trade size statistics (10s)
    if t10:
        sizes = [t.qty for t in t10]
        feats["trade_size_median_10s"] = float(np.median(sizes))
        feats["trade_size_p90_10s"] = float(np.percentile(sizes, 90))
    else:
        feats["trade_size_median_10s"] = 0.0
        feats["trade_size_p90_10s"] = 0.0

    # Дополнительные tape-фичи
    # net_buy_ratio за 10s
    if t10:
        buy_vol = sum(t.qty for t in t10 if not t.is_buyer_maker)
        total_vol = sum(t.qty for t in t10)
        feats["net_buy_ratio_10s"] = buy_vol / total_vol if total_vol > 0 else 0.5
    else:
        feats["net_buy_ratio_10s"] = 0.5

    # large_trade_count_10s (трейды > 2x median)
    if t10 and feats["trade_size_median_10s"] > 0:
        threshold = feats["trade_size_median_10s"] * 2.0
        feats["large_trade_count_10s"] = sum(1 for t in t10 if t.qty > threshold)
    else:
        feats["large_trade_count_10s"] = 0.0


def _fill_tape_defaults(feats: Dict[str, float]):
    for key in [
        "trade_rate_1s", "trade_rate_10s", "trade_rate_30s",
        "signed_vol_10s", "signed_vol_30s",
        "cvd_60s", "cvd_slope",
        "burstiness_10s", "entropy_10s", "switch_rate_10s",
        "trade_size_median_10s", "trade_size_p90_10s",
        "net_buy_ratio_10s", "large_trade_count_10s",
    ]:
        feats[key] = 0.0


# ═══════════════════════════════════════════════════════════════════
# Order Book features
# ═══════════════════════════════════════════════════════════════════

def _orderbook_features(ss: SymbolState, now: float, feats: Dict[str, float]):
    book = ss.futures_book
    cfg = SYMBOL_CONFIGS.get(ss.symbol)
    tick = cfg.tick_size if cfg else 0.01

    if not book.synced or not book.bids or not book.asks:
        _fill_ob_defaults(feats)
        return

    best_bid = max(book.bids.keys())
    best_ask = min(book.asks.keys())
    mid = (best_bid + best_ask) / 2.0

    # spread
    spread_abs = best_ask - best_bid
    spread_ticks = spread_abs / tick if tick > 0 else 0
    spread_bps = (spread_abs / mid) * 10000 if mid > 0 else 0

    feats["spread_ticks"] = spread_ticks
    feats["spread_bps"] = spread_bps
    feats["mid_price"] = mid

    # Сортированные уровни
    sorted_bids = sorted(book.bids.items(), key=lambda x: -x[0])  # убыв. цена
    sorted_asks = sorted(book.asks.items(), key=lambda x: x[0])   # возр. цена

    # imbalance top10 / top20
    top10_bid_vol = sum(q for _, q in sorted_bids[:10])
    top10_ask_vol = sum(q for _, q in sorted_asks[:10])
    top20_bid_vol = sum(q for _, q in sorted_bids[:20])
    top20_ask_vol = sum(q for _, q in sorted_asks[:20])

    feats["imbalance_top10"] = (
        (top10_bid_vol - top10_ask_vol) / (top10_bid_vol + top10_ask_vol)
        if (top10_bid_vol + top10_ask_vol) > 0 else 0.0
    )
    feats["imbalance_top20"] = (
        (top20_bid_vol - top20_ask_vol) / (top20_bid_vol + top20_ask_vol)
        if (top20_bid_vol + top20_ask_vol) > 0 else 0.0
    )

    # liquidity near mid (±10 ticks)
    near_range = tick * 10
    liq_bid_near = sum(q for p, q in book.bids.items() if p >= mid - near_range)
    liq_ask_near = sum(q for p, q in book.asks.items() if p <= mid + near_range)
    feats["liq_near_bid"] = liq_bid_near
    feats["liq_near_ask"] = liq_ask_near
    feats["liq_near_total"] = liq_bid_near + liq_ask_near

    # vacuum_index = liq_near_total / ATR (заполняется позже, пока ставим 0)
    feats["vacuum_index"] = 0.0  # будет обновлено после ATR

    # Wall detection (qty outlier > mean + 3*std)
    all_bid_qtys = [q for _, q in sorted_bids[:50]]
    all_ask_qtys = [q for _, q in sorted_asks[:50]]

    def _detect_wall(prices_qtys: list, side: str) -> tuple:
        """Найти ближайшую стенку. Возвращает (dist_ticks, qty) или (0, 0)."""
        if len(prices_qtys) < 5:
            return 0.0, 0.0
        qtys = [q for _, q in prices_qtys]
        mean_q = np.mean(qtys)
        std_q = np.std(qtys)
        threshold = mean_q + 3 * std_q
        for price, qty in prices_qtys:
            if qty > threshold:
                dist = abs(price - mid) / tick if tick > 0 else 0
                return dist, qty
        return 0.0, 0.0

    bid_wall_dist, bid_wall_qty = _detect_wall(sorted_bids[:50], "bid")
    ask_wall_dist, ask_wall_qty = _detect_wall(sorted_asks[:50], "ask")

    feats["bid_wall_dist_ticks"] = bid_wall_dist
    feats["ask_wall_dist_ticks"] = ask_wall_dist
    feats["bid_wall_qty"] = bid_wall_qty
    feats["ask_wall_qty"] = ask_wall_qty

    # nearest_wall_side: -1 bid, +1 ask, 0 none
    if bid_wall_qty > 0 and ask_wall_qty > 0:
        feats["nearest_wall_side"] = -1.0 if bid_wall_dist < ask_wall_dist else 1.0
    elif bid_wall_qty > 0:
        feats["nearest_wall_side"] = -1.0
    elif ask_wall_qty > 0:
        feats["nearest_wall_side"] = 1.0
    else:
        feats["nearest_wall_side"] = 0.0

    # Replenish score: top5 уровней — среднее qty / среднее qty top 6-20
    if len(sorted_bids) >= 20 and len(sorted_asks) >= 20:
        top5_bid = np.mean([q for _, q in sorted_bids[:5]])
        rest_bid = np.mean([q for _, q in sorted_bids[5:20]])
        top5_ask = np.mean([q for _, q in sorted_asks[:5]])
        rest_ask = np.mean([q for _, q in sorted_asks[5:20]])

        replenish_bid = top5_bid / rest_bid if rest_bid > 0 else 1.0
        replenish_ask = top5_ask / rest_ask if rest_ask > 0 else 1.0
        feats["replenish_score"] = (replenish_bid + replenish_ask) / 2.0
    else:
        feats["replenish_score"] = 1.0

    # Book depth ratio (bid vol / ask vol top 20)
    feats["book_depth_ratio"] = (
        top20_bid_vol / top20_ask_vol if top20_ask_vol > 0 else 1.0
    )


def _fill_ob_defaults(feats: Dict[str, float]):
    for key in [
        "spread_ticks", "spread_bps", "mid_price",
        "imbalance_top10", "imbalance_top20",
        "liq_near_bid", "liq_near_ask", "liq_near_total",
        "vacuum_index",
        "bid_wall_dist_ticks", "ask_wall_dist_ticks",
        "bid_wall_qty", "ask_wall_qty", "nearest_wall_side",
        "replenish_score", "book_depth_ratio",
    ]:
        feats[key] = 0.0


# ═══════════════════════════════════════════════════════════════════
# Spot confirm features
# ═══════════════════════════════════════════════════════════════════

def _spot_features(ss: SymbolState, now: float, feats: Dict[str, float]):
    fut_mid = feats.get("mid_price", 0)
    spot_bt = ss.spot_book_ticker

    # Basis futures - spot
    if fut_mid > 0 and spot_bt.bid_price > 0:
        spot_mid = (spot_bt.bid_price + spot_bt.ask_price) / 2.0
        basis_abs = fut_mid - spot_mid
        basis_bps = (basis_abs / spot_mid) * 10000 if spot_mid > 0 else 0.0
        feats["basis_bps"] = basis_bps
        feats["spot_mid"] = spot_mid
    else:
        feats["basis_bps"] = 0.0
        feats["spot_mid"] = 0.0

    # basis_z_5m: нужна скользящая статистика basis.
    # Упрощение: считаем basis_z как basis_bps / 2.0 (нормировка),
    # полноценный z-score требует rolling буфер, добавим позже.
    feats["basis_z_5m"] = feats["basis_bps"] / 2.0 if abs(feats["basis_bps"]) > 0.01 else 0.0

    # Spot signed vol за 10s
    spot_trades = [t for t in ss.spot_trades if t.ts >= now - 10]
    if spot_trades:
        buy_vol = sum(t.qty * t.price for t in spot_trades if not t.is_buyer_maker)
        sell_vol = sum(t.qty * t.price for t in spot_trades if t.is_buyer_maker)
        feats["spot_signed_vol_10s"] = buy_vol - sell_vol
    else:
        feats["spot_signed_vol_10s"] = 0.0

    # confirm_score 0–100
    # Формула: взвешенная комбинация
    #   - basis_alignment (если fut и spot обе бычьи/медвежьи)
    #   - spot volume agreement
    score = 50.0  # нейтральный

    # Направление futures (signed_vol_10s)
    fut_sv = feats.get("signed_vol_10s", 0)
    spot_sv = feats.get("spot_signed_vol_10s", 0)

    # Если оба в одном направлении — подтверждение
    if fut_sv > 0 and spot_sv > 0:
        score += 25.0
    elif fut_sv < 0 and spot_sv < 0:
        score += 25.0
    elif (fut_sv > 0 and spot_sv < 0) or (fut_sv < 0 and spot_sv > 0):
        score -= 20.0

    # Basis alignment: маленький basis = хорошо
    if abs(feats["basis_bps"]) < 5:
        score += 15.0
    elif abs(feats["basis_bps"]) > 20:
        score -= 15.0

    # Spot активность: есть ли трейды
    if len(spot_trades) > 5:
        score += 10.0

    feats["spot_confirm_score"] = max(0.0, min(100.0, score))


# ═══════════════════════════════════════════════════════════════════
# Volatility features
# ═══════════════════════════════════════════════════════════════════

def _volatility_features(ss: SymbolState, now: float, feats: Dict[str, float]):
    """ATR_1m(14) из последних закрытых 1m свечей."""
    klines = [k for k in ss.futures_klines if k.is_closed]

    if len(klines) < 2:
        feats["atr_1m_14"] = 0.0
        return

    # True Range для каждой свечи
    trs = []
    for i in range(1, len(klines)):
        prev_close = klines[i - 1].close
        k = klines[i]
        tr = max(
            k.high - k.low,
            abs(k.high - prev_close),
            abs(k.low - prev_close),
        )
        trs.append(tr)

    # ATR(14) — EMA или простое среднее последних 14
    period = min(14, len(trs))
    atr = sum(trs[-period:]) / period if period > 0 else 0.0
    feats["atr_1m_14"] = atr

    # Обновляем vacuum_index теперь когда ATR доступен
    if atr > 0:
        feats["vacuum_index"] = feats.get("liq_near_total", 0) / atr

    # Дополнительные vol-фичи
    # Relative ATR (bps)
    mid = feats.get("mid_price", 0)
    feats["atr_bps"] = (atr / mid) * 10000 if mid > 0 else 0.0

    # Kline body ratio (последняя закрытая свеча)
    if klines:
        last = klines[-1]
        body = abs(last.close - last.open)
        wick = last.high - last.low
        feats["kline_body_ratio"] = body / wick if wick > 0 else 0.0
    else:
        feats["kline_body_ratio"] = 0.0


# ═══════════════════════════════════════════════════════════════════
# Список имён всех фич (для обучения)
# ═══════════════════════════════════════════════════════════════════

FEATURE_NAMES = [
    # Tape (14)
    "trade_rate_1s", "trade_rate_10s", "trade_rate_30s",
    "signed_vol_10s", "signed_vol_30s",
    "cvd_60s", "cvd_slope",
    "burstiness_10s", "entropy_10s", "switch_rate_10s",
    "trade_size_median_10s", "trade_size_p90_10s",
    "net_buy_ratio_10s", "large_trade_count_10s",
    # Order Book (16)
    "spread_ticks", "spread_bps", "mid_price",
    "imbalance_top10", "imbalance_top20",
    "liq_near_bid", "liq_near_ask", "liq_near_total",
    "vacuum_index",
    "bid_wall_dist_ticks", "ask_wall_dist_ticks",
    "bid_wall_qty", "ask_wall_qty", "nearest_wall_side",
    "replenish_score", "book_depth_ratio",
    # Spot (5)
    "basis_bps", "spot_mid", "basis_z_5m",
    "spot_signed_vol_10s", "spot_confirm_score",
    # Volatility (4)
    "atr_1m_14", "atr_bps", "kline_body_ratio",
]

# Фичи для модели (исключаем mid_price и spot_mid — они не фичи, а raw prices)
MODEL_FEATURE_NAMES = [f for f in FEATURE_NAMES if f not in ("mid_price", "spot_mid")]
