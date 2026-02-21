"""
Master Gate — фильтрует сигналы по набору условий.
"""

from typing import List, Tuple

from scalping.core.logger import get_logger
from scalping.core.state import SymbolState
from scalping.core.config import (
    GATE_MAX_SPREAD_TICKS,
    GATE_MAX_WS_LAG_MS,
    GATE_PROB_THRESHOLD,
    GATE_MIN_SPOT_CONFIRM,
    GATE_MAX_RESYNC_10M,
)

log = get_logger("gate")


def check_gate(ss: SymbolState, prob: float) -> Tuple[bool, List[str]]:
    """
    Проверить Master Gate.
    Returns: (allowed: bool, reasons: list[str])
    Если allowed=False, reasons содержит причины блокировки.
    """
    reasons: List[str] = []
    feats = ss.features

    # 1. Spread check
    spread_ticks = feats.get("spread_ticks", 999)
    if spread_ticks > GATE_MAX_SPREAD_TICKS:
        reasons.append(f"SPREAD: {spread_ticks:.1f} > {GATE_MAX_SPREAD_TICKS} ticks")

    # 2. WS lag check
    lag_p95 = ss.health.ws_lag_p95()
    if lag_p95 > GATE_MAX_WS_LAG_MS:
        reasons.append(f"WS_LAG: p95={lag_p95:.0f}ms > {GATE_MAX_WS_LAG_MS}ms")

    # 3. Depth sync check
    if not ss.health.depth_sync_ok:
        reasons.append("DEPTH_DESYNC: order book not synchronized")

    # 4. Resync count check
    ss.health.record_resync  # обновляем счётчик (уже считается в depth_manager)
    if ss.health.resync_count_10m > GATE_MAX_RESYNC_10M:
        reasons.append(f"RESYNC: {ss.health.resync_count_10m} > {GATE_MAX_RESYNC_10M} in 10m")

    # 5. Probability threshold
    if prob < GATE_PROB_THRESHOLD:
        reasons.append(f"PROB: {prob:.3f} < {GATE_PROB_THRESHOLD}")

    # 6. Spot confirm check
    spot_score = feats.get("spot_confirm_score", 0)
    if spot_score < GATE_MIN_SPOT_CONFIRM:
        reasons.append(f"SPOT_CONFIRM: {spot_score:.0f} < {GATE_MIN_SPOT_CONFIRM}")

    allowed = len(reasons) == 0
    return allowed, reasons
