"""
Feature Loop — вычисляет фичи каждую секунду и сохраняет в state + parquet.
"""

import asyncio
import time
from datetime import datetime, timezone

import pandas as pd

from scalping.core.logger import get_logger
from scalping.core.state import SharedState
from scalping.core.config import SYMBOLS, FEATURE_INTERVAL_SEC, FEATURES_DIR
from scalping.features.engine import compute_features, FEATURE_NAMES

log = get_logger("feature_loop")


class FeatureLoop:
    """Периодический расчёт фич для всех символов."""

    def __init__(self, state: SharedState):
        self.state = state
        self._buffers = {s: [] for s in SYMBOLS}  # накопитель для parquet
        self._flush_interval = 300  # каждые 5 минут сбрасываем в parquet
        self._last_flush = time.time()

    async def run(self):
        log.info("Feature loop started (interval=%.1fs)", FEATURE_INTERVAL_SEC)
        shutdown = self.state.shutdown_event

        while not shutdown.is_set():
            loop_start = time.time()

            for symbol in SYMBOLS:
                try:
                    ss = self.state.get(symbol)
                    now = time.time()
                    feats = compute_features(ss, now)

                    # Сохраняем в state
                    ss.features = feats

                    # Добавляем в буфер для parquet
                    row = {"timestamp": now, "symbol": symbol}
                    row.update(feats)
                    self._buffers[symbol].append(row)

                except Exception as e:
                    log.exception("Feature computation error for %s: %s", symbol, e)

            # Flush в parquet периодически
            if time.time() - self._last_flush > self._flush_interval:
                await self._flush_to_parquet()

            # Sleep до следующего тика
            elapsed = time.time() - loop_start
            sleep_time = max(0.01, FEATURE_INTERVAL_SEC - elapsed)
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=sleep_time)
                break  # shutdown
            except asyncio.TimeoutError:
                pass

        # Final flush
        await self._flush_to_parquet()
        log.info("Feature loop stopped")

    async def _flush_to_parquet(self):
        """Сбросить накопленные фичи в parquet файлы."""
        for symbol in SYMBOLS:
            buf = self._buffers[symbol]
            if not buf:
                continue

            try:
                df = pd.DataFrame(buf)
                date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
                path = FEATURES_DIR / f"features_{symbol}_{date_str}.parquet"

                if path.exists():
                    existing = pd.read_parquet(path)
                    df = pd.concat([existing, df], ignore_index=True)

                df.to_parquet(path, index=False)
                log.info("Flushed %d feature rows for %s to %s", len(buf), symbol, path.name)
                self._buffers[symbol] = []
            except Exception as e:
                log.exception("Failed to flush features for %s: %s", symbol, e)

        self._last_flush = time.time()
