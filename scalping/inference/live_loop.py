"""
Live Inference Loop — каждую секунду делает предсказание + master gate.
"""

import asyncio
import time

from scalping.core.logger import get_logger
from scalping.core.state import SharedState
from scalping.core.config import SYMBOLS
from scalping.inference.predictor import Predictor
from scalping.inference.gate import check_gate

log = get_logger("live_loop")


class LiveInferenceLoop:
    """Периодический inference для всех символов."""

    def __init__(self, state: SharedState):
        self.state = state
        self.predictors = {}

        # Загружаем модели
        for symbol in SYMBOLS:
            pred = Predictor()
            pred.load_model(symbol)
            self.predictors[symbol] = pred

        if any(p.loaded for p in self.predictors.values()):
            state.model_loaded = True

    async def run(self):
        log.info("Live inference loop started")
        shutdown = self.state.shutdown_event

        while not shutdown.is_set():
            for symbol in SYMBOLS:
                try:
                    ss = self.state.get(symbol)
                    pred = self.predictors[symbol]

                    # Predict
                    prob = pred.predict(ss.features)
                    ss.prob_tp_first = prob

                    # Gate check
                    allowed, reasons = check_gate(ss, prob)
                    ss.gate_allowed = allowed
                    ss.gate_reasons = reasons

                except Exception as e:
                    log.exception("Inference error for %s: %s", symbol, e)

            try:
                await asyncio.wait_for(shutdown.wait(), timeout=1.0)
                break
            except asyncio.TimeoutError:
                pass

        log.info("Live inference loop stopped")
