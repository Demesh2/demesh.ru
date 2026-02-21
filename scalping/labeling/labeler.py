"""
TP-first Labeling — размечает данные для обучения.

Горизонт H = 60 секунд.
Барьеры на основе ATR:
  - D_eff = k * ATR_1m(14) + buffer
  - buffer = max(2 * tick_size, 0.05 * ATR)
  - BTC: k = 0.25, ETH: k = 0.30

Label:
  - 1: TP достигнут раньше SL
  - 0: SL достигнут раньше TP
  - TIMEOUT: ни TP ни SL за H секунд — пропускаем (исключаем)
"""

import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from scalping.core.logger import get_logger
from scalping.core.config import (
    SYMBOL_CONFIGS,
    LABEL_HORIZON_SEC,
    LABELS_DIR,
    FEATURES_DIR,
    PAPER_FEE_BPS,
    PAPER_SLIPPAGE_BPS,
)

log = get_logger("labeler")


def compute_barriers(atr: float, symbol: str) -> tuple:
    """
    Вычислить TP и SL барьеры.
    Returns: (d_eff, tp_long, sl_long) — расстояния от mid в абсолютных единицах.
    """
    cfg = SYMBOL_CONFIGS.get(symbol)
    if cfg is None:
        raise ValueError(f"Unknown symbol: {symbol}")

    tick = cfg.tick_size
    k = cfg.k_atr

    buffer = max(2 * tick, 0.05 * atr)
    d_eff = k * atr + buffer

    return d_eff


def label_features_file(features_path: Path, symbol: str) -> Optional[Path]:
    """
    Прочитать файл фич, разметить TP-first лейблами.
    Сохранить в data/labels/.

    Работает офлайн: берёт mid_price и atr_1m_14 из фич,
    симулирует TP/SL по последующим mid_price.
    """
    if not features_path.exists():
        log.warning("Features file not found: %s", features_path)
        return None

    df = pd.read_parquet(features_path)
    if df.empty:
        return None

    df = df[df["symbol"] == symbol].copy() if "symbol" in df.columns else df.copy()
    if df.empty:
        return None

    df = df.sort_values("timestamp").reset_index(drop=True)

    mid_prices = df["mid_price"].values
    timestamps = df["timestamp"].values
    atrs = df["atr_1m_14"].values

    labels = []
    horizon = LABEL_HORIZON_SEC

    for i in range(len(df)):
        if atrs[i] <= 0 or mid_prices[i] <= 0:
            labels.append(np.nan)
            continue

        d_eff = compute_barriers(atrs[i], symbol)
        entry = mid_prices[i]
        tp_price = entry + d_eff
        sl_price = entry - d_eff
        t_start = timestamps[i]
        t_end = t_start + horizon

        # Ищем будущие точки в горизонте
        label_value = np.nan  # timeout default

        for j in range(i + 1, len(df)):
            if timestamps[j] > t_end:
                break

            p = mid_prices[j]

            # LONG сценарий: TP наверх, SL вниз
            if p >= tp_price:
                label_value = 1.0
                break
            elif p <= sl_price:
                label_value = 0.0
                break

        labels.append(label_value)

    df["label"] = labels
    df["d_eff"] = df["atr_1m_14"].apply(lambda a: compute_barriers(a, symbol) if a > 0 else 0)

    # Убираем TIMEOUT (NaN)
    df_labeled = df.dropna(subset=["label"]).copy()
    df_labeled["label"] = df_labeled["label"].astype(int)

    if df_labeled.empty:
        log.warning("No labeled samples (all timeout) for %s", features_path.name)
        return None

    # Сохраняем
    date_str = features_path.stem.split("_")[-1]  # features_BTCUSDT_20260221 -> 20260221
    out_path = LABELS_DIR / f"labels_{symbol}_{date_str}.parquet"
    df_labeled.to_parquet(out_path, index=False)

    n_pos = (df_labeled["label"] == 1).sum()
    n_neg = (df_labeled["label"] == 0).sum()
    log.info(
        "Labeled %s: %d samples (TP=%d, SL=%d, ratio=%.2f), saved to %s",
        symbol, len(df_labeled), n_pos, n_neg,
        n_pos / max(1, n_pos + n_neg), out_path.name,
    )
    return out_path


def label_all_features(symbol: str):
    """Разметить все файлы фич для символа."""
    pattern = f"features_{symbol}_*.parquet"
    files = sorted(FEATURES_DIR.glob(pattern))
    if not files:
        log.warning("No feature files found for %s", symbol)
        return

    for f in files:
        try:
            label_features_file(f, symbol)
        except Exception as e:
            log.exception("Error labeling %s: %s", f.name, e)
