"""
Конфигурация проекта. Загружает .env и предоставляет все параметры.
"""

import os
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict

# Корень проекта — папка где лежит run.py
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

DATA_DIR = PROJECT_ROOT / "data"
FEATURES_DIR = DATA_DIR / "features"
LABELS_DIR = DATA_DIR / "labels"
REPORTS_DIR = DATA_DIR / "reports"
MODELS_DIR = DATA_DIR / "models"
LOGS_DIR = PROJECT_ROOT / "logs"

# Создаём директории при импорте
for d in [FEATURES_DIR, LABELS_DIR, REPORTS_DIR, MODELS_DIR, LOGS_DIR]:
    d.mkdir(parents=True, exist_ok=True)


def _load_dotenv():
    """Простая загрузка .env без зависимостей."""
    env_path = PROJECT_ROOT / ".env"
    if not env_path.exists():
        return
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip("'\"")
            if key not in os.environ:
                os.environ[key] = value


_load_dotenv()

# ─── Символы ───────────────────────────────────────────────────────
SYMBOLS = ["BTCUSDT", "ETHUSDT"]

# ─── Binance API (только для REST snapshot) ────────────────────────
BINANCE_API_KEY = os.environ.get("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.environ.get("BINANCE_API_SECRET", "")

# ─── Binance endpoints ────────────────────────────────────────────
FUTURES_WS_BASE = "wss://fstream.binance.com"
FUTURES_REST_BASE = "https://fapi.binance.com"
SPOT_WS_BASE = "wss://stream.binance.com:9443"
SPOT_REST_BASE = "https://api.binance.com"

# ─── Depth snapshot ───────────────────────────────────────────────
DEPTH_SNAPSHOT_LIMIT = 1000  # количество уровней в REST snapshot

# ─── Feature engine ──────────────────────────────────────────────
FEATURE_INTERVAL_SEC = 1.0  # как часто пересчитывать фичи

# ─── Labeling ─────────────────────────────────────────────────────
LABEL_HORIZON_SEC = 60
LABEL_ATR_PERIOD = 14  # ATR(14) на 1m свечах


@dataclass
class SymbolConfig:
    """Параметры для конкретного символа."""
    symbol: str
    tick_size: float
    k_atr: float  # множитель ATR для TP/SL


SYMBOL_CONFIGS: Dict[str, SymbolConfig] = {
    "BTCUSDT": SymbolConfig(symbol="BTCUSDT", tick_size=0.10, k_atr=0.25),
    "ETHUSDT": SymbolConfig(symbol="ETHUSDT", tick_size=0.01, k_atr=0.30),
}

# ─── Master Gate ──────────────────────────────────────────────────
GATE_MAX_SPREAD_TICKS = 2
GATE_MAX_WS_LAG_MS = 800
GATE_PROB_THRESHOLD = 0.70
GATE_MIN_SPOT_CONFIRM = 60
GATE_MAX_RESYNC_10M = 5

# ─── Paper Trading ───────────────────────────────────────────────
PAPER_FEE_BPS = 4.0       # 0.04% (maker+taker в одну сторону ≈ 2bps, туда-обратно 4)
PAPER_SLIPPAGE_BPS = 1.0  # дополнительный buffer
PAPER_MAX_TRADES_MEM = 200  # хранить последние N сделок в памяти

# ─── Web panel ────────────────────────────────────────────────────
WEB_HOST = os.environ.get("WEB_HOST", "127.0.0.1")
WEB_PORT = int(os.environ.get("WEB_PORT", "8080"))

# ─── Logging ──────────────────────────────────────────────────────
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
LOG_MAX_BYTES = 10 * 1024 * 1024  # 10 MB
LOG_BACKUP_COUNT = 5

# ─── LightGBM defaults ───────────────────────────────────────────
LGBM_PARAMS = {
    "objective": "binary",
    "metric": "binary_logloss",
    "n_estimators": 400,
    "learning_rate": 0.05,
    "num_leaves": 64,
    "min_data_in_leaf": 100,
    "feature_fraction": 0.8,
    "bagging_fraction": 0.8,
    "bagging_freq": 1,
    "lambda_l2": 1.0,
    "verbose": -1,
}
