"""
Live Inference — загружает модель и делает предсказания в реальном времени.
"""

import pickle
from pathlib import Path
from typing import Optional, List

import numpy as np

from scalping.core.logger import get_logger
from scalping.core.config import MODELS_DIR
from scalping.core.state import SharedState, SymbolState
from scalping.features.engine import MODEL_FEATURE_NAMES

log = get_logger("predictor")


class Predictor:
    """Загружает LightGBM модель и делает inference."""

    def __init__(self):
        self.model = None
        self.features: List[str] = []
        self.model_symbol: str = ""
        self.loaded = False

    def load_model(self, symbol: str) -> bool:
        """Загрузить модель для символа."""
        # Пробуем символо-специфичную модель, потом общую
        paths = [
            MODELS_DIR / f"tpfirst_{symbol.lower()}_lgbm.pkl",
            MODELS_DIR / "tpfirst_lgbm.pkl",
        ]

        for path in paths:
            if path.exists():
                try:
                    with open(path, "rb") as f:
                        data = pickle.load(f)
                    self.model = data["model"]
                    self.features = data["features"]
                    self.model_symbol = data.get("symbol", symbol)
                    self.loaded = True
                    log.info(
                        "Model loaded from %s (%d features, trained for %s)",
                        path.name, len(self.features), self.model_symbol,
                    )
                    return True
                except Exception as e:
                    log.exception("Failed to load model from %s: %s", path, e)

        log.warning("No model found for %s", symbol)
        return False

    def predict(self, features: dict) -> float:
        """
        Предсказать prob_tp_first.
        Returns: вероятность от 0 до 1, или 0.5 если модель не загружена.
        """
        if not self.loaded or self.model is None:
            return 0.5

        # Собираем фичи в правильном порядке
        x = []
        for fname in self.features:
            x.append(features.get(fname, 0.0))

        x = np.array([x])
        try:
            prob = self.model.predict_proba(x)[0, 1]
            return float(prob)
        except Exception as e:
            log.warning("Prediction error: %s", e)
            return 0.5
