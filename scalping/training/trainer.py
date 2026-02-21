"""
LightGBM Training — offline обучение модели.

Walk-forward по дням (time split).
Метрика: EV after fees + поиск оптимального threshold.
"""

import pickle
from pathlib import Path
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd
import lightgbm as lgb
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    log_loss,
    roc_auc_score,
)

from scalping.core.logger import get_logger
from scalping.core.config import (
    LGBM_PARAMS,
    LABELS_DIR,
    MODELS_DIR,
    SYMBOL_CONFIGS,
    PAPER_FEE_BPS,
    PAPER_SLIPPAGE_BPS,
)
from scalping.features.engine import MODEL_FEATURE_NAMES

log = get_logger("trainer")


def load_labeled_data(symbol: str) -> pd.DataFrame:
    """Загрузить все размеченные данные для символа."""
    pattern = f"labels_{symbol}_*.parquet"
    files = sorted(LABELS_DIR.glob(pattern))
    if not files:
        log.warning("No label files for %s", symbol)
        return pd.DataFrame()

    dfs = [pd.read_parquet(f) for f in files]
    df = pd.concat(dfs, ignore_index=True)
    df = df.sort_values("timestamp").reset_index(drop=True)
    log.info("Loaded %d labeled samples for %s from %d files", len(df), symbol, len(files))
    return df


def train_model(symbol: str, save: bool = True) -> Optional[lgb.LGBMClassifier]:
    """
    Обучить модель для символа.
    Walk-forward split по дням (80% train, 20% val по времени).
    """
    df = load_labeled_data(symbol)
    if df.empty:
        log.error("No data to train for %s", symbol)
        return None

    # Отфильтруем только доступные фичи
    available_features = [f for f in MODEL_FEATURE_NAMES if f in df.columns]
    if not available_features:
        log.error("No features available in data for %s", symbol)
        return None

    log.info("Training with %d features: %s", len(available_features), available_features[:5])

    X = df[available_features].values
    y = df["label"].values
    timestamps = df["timestamp"].values

    # Time split: 80% train, 20% test
    split_idx = int(len(df) * 0.8)
    X_train, X_val = X[:split_idx], X[split_idx:]
    y_train, y_val = y[:split_idx], y[split_idx:]

    if len(X_train) < 200 or len(X_val) < 50:
        log.warning("Too few samples: train=%d, val=%d", len(X_train), len(X_val))

    log.info(
        "Train: %d samples (%.1f%% positive), Val: %d samples (%.1f%% positive)",
        len(y_train), 100.0 * y_train.mean(),
        len(y_val), 100.0 * y_val.mean(),
    )

    # Train LightGBM
    model = lgb.LGBMClassifier(**LGBM_PARAMS)
    model.fit(
        X_train, y_train,
        eval_set=[(X_val, y_val)],
        callbacks=[
            lgb.log_evaluation(50),
            lgb.early_stopping(50, verbose=True),
        ],
    )

    # Evaluate
    probs = model.predict_proba(X_val)[:, 1]
    _evaluate_model(y_val, probs, symbol)

    # Feature importance
    importance = sorted(
        zip(available_features, model.feature_importances_),
        key=lambda x: -x[1],
    )
    log.info("Top 10 features:")
    for fname, imp in importance[:10]:
        log.info("  %s: %d", fname, imp)

    # Save
    if save:
        model_data = {
            "model": model,
            "features": available_features,
            "symbol": symbol,
            "n_train": len(y_train),
            "n_val": len(y_val),
        }
        model_path = MODELS_DIR / f"tpfirst_{symbol.lower()}_lgbm.pkl"
        with open(model_path, "wb") as f:
            pickle.dump(model_data, f)
        log.info("Model saved to %s", model_path)

        # Также сохраняем общую модель
        general_path = MODELS_DIR / "tpfirst_lgbm.pkl"
        with open(general_path, "wb") as f:
            pickle.dump(model_data, f)

    return model


def _evaluate_model(y_true: np.ndarray, probs: np.ndarray, symbol: str):
    """Оценка модели: AUC, logloss, EV для разных threshold."""
    try:
        auc = roc_auc_score(y_true, probs)
    except ValueError:
        auc = 0.0

    ll = log_loss(y_true, probs)
    log.info("[%s] AUC=%.4f, LogLoss=%.4f", symbol, auc, ll)

    # EV after fees для разных threshold
    cfg = SYMBOL_CONFIGS.get(symbol)
    total_fee_bps = PAPER_FEE_BPS + PAPER_SLIPPAGE_BPS  # ~5 bps

    log.info("[%s] Threshold analysis:", symbol)
    for threshold in [0.55, 0.60, 0.65, 0.70, 0.75, 0.80]:
        mask = probs >= threshold
        if mask.sum() < 10:
            continue

        y_sel = y_true[mask]
        n_signals = mask.sum()
        win_rate = y_sel.mean()

        # EV: win_rate * avg_win - (1 - win_rate) * avg_loss - fees
        # Упрощённо: win/loss одинаковые (симметричные барьеры d_eff)
        # EV per trade = win_rate * d_eff - (1-win_rate) * d_eff - fees
        # EV = d_eff * (2*win_rate - 1) - fees
        # Нормируем в bps (d_eff уже учтён в ATR*k)
        # Средний d_eff_bps ≈ k * ATR_bps ≈ 25-30 bps для BTC
        avg_d_eff_bps = 25 if symbol == "BTCUSDT" else 30
        ev_bps = avg_d_eff_bps * (2 * win_rate - 1) - total_fee_bps

        log.info(
            "  thr=%.2f: signals=%d (%.1f%%), WR=%.1f%%, EV≈%.1f bps",
            threshold, n_signals, 100.0 * n_signals / len(probs),
            100.0 * win_rate, ev_bps,
        )


def train_all():
    """Обучить модели для всех символов."""
    from scalping.core.config import SYMBOLS
    for symbol in SYMBOLS:
        log.info("=" * 60)
        log.info("Training model for %s", symbol)
        log.info("=" * 60)
        train_model(symbol)
