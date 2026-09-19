from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict
from statistics import mean
from typing import Sequence

from sentiment_scanner.binance import Kline
from .agent import autonomous_research
from .engine import QuantConfig


def _candidate_key(config: dict) -> tuple:
    base = asdict(QuantConfig())
    return tuple((k, config.get(k, base[k])) for k in sorted(base))


def universe_research(datasets: dict[str, Sequence[Kline]], champion: QuantConfig | None = None) -> dict:
    """Cross-market autonomous research.

    The same hypotheses are tested on every symbol. A configuration is only a
    promotion candidate when improvement is broad rather than driven by one coin.
    """
    cfg = champion or QuantConfig()
    symbol_results = []
    votes: Counter[tuple] = Counter()
    configs: dict[tuple, dict] = {}
    scores: defaultdict[tuple, list[float]] = defaultdict(list)
    exps: defaultdict[tuple, list[float]] = defaultdict(list)
    dds: defaultdict[tuple, list[float]] = defaultdict(list)

    for symbol, rows in datasets.items():
        if len(rows) < 500:
            symbol_results.append({"symbol": symbol, "error": "insufficient_history", "bars": len(rows)})
            continue
        result = autonomous_research(rows, cfg)
        symbol_results.append({
            "symbol": symbol, "bars": len(rows), "decision": result["decision"],
            "champion_oos": result["champion"]["oos"],
            "promotion_candidate": result["promotion_candidate"],
            "findings": result["diagnosis"]["findings"],
        })
        for challenger in result["challengers"]:
            key = _candidate_key(challenger["config"])
            configs[key] = challenger["config"]
            scores[key].append(float(challenger["oos_score"]))
            exps[key].append(float(challenger["oos"]["expectancy_r"]))
            dds[key].append(float(challenger["oos"]["max_drawdown_r"]))
            if challenger["passes_gate"]:
                votes[key] += 1

    tested = max(1, sum(1 for x in symbol_results if "error" not in x))
    candidates = []
    for key, config in configs.items():
        n = len(scores[key])
        positive = sum(1 for x in exps[key] if x > 0)
        pass_rate = votes[key] / tested
        positive_rate = positive / max(1, n)
        candidates.append({
            "config": config, "symbols_tested": n, "gate_votes": votes[key],
            "gate_rate": round(pass_rate, 3), "positive_oos_rate": round(positive_rate, 3),
            "mean_oos_score": round(mean(scores[key]), 4),
            "mean_oos_expectancy_r": round(mean(exps[key]), 4),
            "mean_oos_drawdown_r": round(mean(dds[key]), 4),
            "universe_pass": bool(tested >= 5 and pass_rate >= .50 and positive_rate >= .60),
        })
    candidates.sort(key=lambda x: (x["universe_pass"], x["gate_rate"], x["mean_oos_score"]), reverse=True)
    promotion = next((x for x in candidates if x["universe_pass"]), None)
    finding_counts = Counter(f for x in symbol_results if "error" not in x for f in x["findings"])
    return {
        "mode": "quant-v4-universe-agent", "production_mutation": False,
        "symbols_requested": len(datasets), "symbols_tested": tested,
        "champion_config": asdict(cfg),
        "market_findings": [{"finding": k, "symbols": v} for k, v in finding_counts.most_common()],
        "candidates": candidates,
        "promotion_candidate": promotion,
        "decision": "universe_candidate_ready_for_review" if promotion else "keep_champion",
        "symbol_results": symbol_results,
        "guardrails": [
            "One shared strategy across symbols; no per-coin curve fitting",
            "Chronological OOS validation per symbol",
            "Candidate requires broad cross-market gate support",
            "At least 60% of tested symbols require positive OOS expectancy",
            "Research cannot mutate the live production strategy",
        ],
    }
