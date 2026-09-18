from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from sentiment_scanner.binance import Kline


@dataclass(frozen=True)
class TpoProfile:
    poc: float
    vah: float
    val: float
    total_tpo: int
    tick_size: float


def tpo_profile(rows: Sequence[Kline], bins: int = 48, value_area_pct: float = 0.70) -> TpoProfile | None:
    """Approximate TPO from candle ranges using equal price bins.

    Each candle contributes one TPO to every price bin touched by its high-low
    range. Value area expands from POC toward the side with the larger adjacent
    TPO count until the requested percentage is covered.
    """
    if not rows or bins < 8:
        return None
    lo = min(r.low for r in rows)
    hi = max(r.high for r in rows)
    span = hi - lo
    if span <= 0:
        return None
    tick = span / bins
    counts = [0] * bins
    for row in rows:
        first = max(0, min(bins - 1, int((row.low - lo) / tick)))
        last = max(0, min(bins - 1, int((row.high - lo) / tick)))
        for idx in range(first, last + 1):
            counts[idx] += 1
    total = sum(counts)
    if not total:
        return None
    poc_idx = max(range(bins), key=lambda i: counts[i])
    target = total * value_area_pct
    covered = counts[poc_idx]
    left = right = poc_idx
    while covered < target and (left > 0 or right < bins - 1):
        left_count = counts[left - 1] if left > 0 else -1
        right_count = counts[right + 1] if right < bins - 1 else -1
        if right_count >= left_count and right < bins - 1:
            right += 1
            covered += counts[right]
        elif left > 0:
            left -= 1
            covered += counts[left]
        else:
            break
    center = lambda idx: lo + (idx + 0.5) * tick
    return TpoProfile(poc=center(poc_idx), vah=lo + (right + 1) * tick, val=lo + left * tick, total_tpo=total, tick_size=tick)
