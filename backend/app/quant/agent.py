from __future__ import annotations

from dataclasses import asdict, replace
from statistics import mean
from typing import Sequence

from sentiment_scanner.binance import Kline
from .engine import QuantConfig, run_quant_v1
from .research import _metrics


def _score(metrics: dict) -> float:
    """Robustness-oriented research score, never used as a live entry signal."""
    n = int(metrics.get("trades", 0))
    if n < 3:
        return -999.0
    exp = float(metrics.get("expectancy_r", 0))
    sharpe = max(-5.0, min(5.0, float(metrics.get("sharpe_r", 0))))
    dd = float(metrics.get("max_drawdown_r", 0))
    sample_penalty = max(0.0, (20 - n) / 20)
    return round(exp * 4 + sharpe * 0.35 - dd * 0.035 - sample_penalty, 5)


def diagnose(trades: Sequence[dict]) -> dict:
    if not trades:
        return {"findings": ["No closed trades; loosen setup only through controlled experiments."],
                "sl_reasons": {}, "segments": {}}
    reasons: dict[str, int] = {}
    for t in trades:
        if t.get("exit_reason") == "SL":
            for r in str(t.get("sl_reason") or "structure_failure").split(","):
                reasons[r] = reasons.get(r, 0) + 1
    def seg(name, pred):
        xs = [t for t in trades if pred(t)]
        return {"name": name, **_metrics(xs)}
    segments = {
        "high_overlap": seg("VA overlap >= 50%", lambda t: float(t.get("va_overlap", 0)) >= .5),
        "low_overlap": seg("VA overlap < 50%", lambda t: float(t.get("va_overlap", 0)) < .5),
        "strong_volume": seg("Volume >= 1.3x", lambda t: float(t.get("volume_ratio", 0)) >= 1.3),
        "weak_volume": seg("Volume < 1.3x", lambda t: float(t.get("volume_ratio", 0)) < 1.3),
        "long": seg("Long", lambda t: t.get("side") == "long"),
        "short": seg("Short", lambda t: t.get("side") == "short"),
    }
    findings = []
    ho, lo = segments["high_overlap"], segments["low_overlap"]
    if ho["trades"] >= 3 and lo["trades"] >= 3 and ho["expectancy_r"] < lo["expectancy_r"]:
        findings.append("High Value-Area overlap underperforms lower-overlap trades; test stricter regime filtering.")
    sv, wv = segments["strong_volume"], segments["weak_volume"]
    if sv["trades"] >= 3 and wv["trades"] >= 3 and sv["expectancy_r"] > wv["expectancy_r"]:
        findings.append("Higher relative volume has better expectancy; test a higher volume threshold.")
    avg_mfe = mean(float(t.get("mfe_r", 0)) for t in trades)
    avg_mae = mean(float(t.get("mae_r", 0)) for t in trades)
    if avg_mfe > 2 and avg_mae < .8:
        findings.append("Trades show favorable excursion relative to adverse excursion; test exit/stop variants without changing entries.")
    if not findings:
        findings.append("No dominant failure mode found; prefer small one-variable experiments over broad parameter changes.")
    return {"findings": findings, "sl_reasons": reasons, "segments": segments,
            "avg_mfe_r": round(avg_mfe, 3), "avg_mae_r": round(avg_mae, 3)}


def propose(config: QuantConfig, diagnosis: dict) -> list[dict]:
    """Generate interpretable challengers. Each proposal changes one hypothesis."""
    candidates = [
        ("volume_stricter", "Require stronger participation", replace(config, volume_multiplier=min(1.6, config.volume_multiplier + .1))),
        ("volume_looser", "Check whether volume filter is suppressing useful trades", replace(config, volume_multiplier=max(1.0, config.volume_multiplier - .1))),
        ("stop_wider", "Test whether normal adverse excursion is hitting the stop", replace(config, stop_atr=min(1.5, config.stop_atr + .25))),
        ("stop_tighter", "Test capital efficiency and faster invalidation", replace(config, stop_atr=max(.25, config.stop_atr - .25))),
        ("acceptance_stricter", "Require one more confirming close", replace(config, acceptance_bars=min(3, config.acceptance_bars + 1))),
        ("balance_stricter", "Classify more overlapping profiles as balance", replace(config, balance_overlap=max(.35, config.balance_overlap - .10))),
    ]
    return [{"id": cid, "hypothesis": why, "config": asdict(cfg)} for cid, why, cfg in candidates]


def _config(payload: dict) -> QuantConfig:
    allowed = QuantConfig.__dataclass_fields__.keys()
    return QuantConfig(**{k: v for k, v in payload.items() if k in allowed})


def _evaluate(rows: Sequence[Kline], cfg: QuantConfig, train_ratio: float = .65) -> dict:
    split = max(250, min(len(rows)-100, int(len(rows)*train_ratio)))
    train, oos = rows[:split], rows[split:]
    is_result = run_quant_v1(train, cfg)
    oos_result = run_quant_v1(oos, cfg)
    ism, oom = _metrics(is_result.get("trades", [])), _metrics(oos_result.get("trades", []))
    return {"is": ism, "oos": oom, "is_score": _score(ism), "oos_score": _score(oom),
            "generalization_gap": round(float(ism["expectancy_r"])-float(oom["expectancy_r"]), 4)}


def autonomous_research(rows: Sequence[Kline], champion: QuantConfig | None = None) -> dict:
    """One bounded autonomous research cycle.

    The agent may diagnose, propose and test challengers, but it never changes
    production trading rules. Promotion is recommendation-only.
    """
    cfg = champion or QuantConfig()
    baseline_full = run_quant_v1(rows, cfg)
    diagnosis = diagnose(baseline_full.get("trades", []))
    baseline = _evaluate(rows, cfg)
    challengers = []
    for proposal in propose(cfg, diagnosis):
        ev = _evaluate(rows, _config(proposal["config"]))
        improvement = round(ev["oos_score"] - baseline["oos_score"], 5)
        enough_oos = ev["oos"]["trades"] >= 5
        stable = ev["oos"]["expectancy_r"] > 0 and ev["oos"]["max_drawdown_r"] <= max(2.0, baseline["oos"]["max_drawdown_r"] * 1.25)
        challengers.append({**proposal, **ev, "improvement": improvement,
                            "passes_gate": bool(enough_oos and stable and improvement > .05)})
    challengers.sort(key=lambda x: x["oos_score"], reverse=True)
    passing = [x for x in challengers if x["passes_gate"]]
    recommendation = passing[0] if passing else None
    return {
        "mode": "autonomous-research-v3",
        "production_mutation": False,
        "champion": {"config": asdict(cfg), **baseline},
        "diagnosis": diagnosis,
        "challengers": challengers,
        "promotion_candidate": recommendation,
        "decision": "candidate_ready_for_review" if recommendation else "keep_champion",
        "guardrails": [
            "No automatic production strategy mutation",
            "Chronological IS/OOS split",
            "Minimum OOS sample gate",
            "Positive OOS expectancy required",
            "Drawdown deterioration capped",
            "One-hypothesis challengers retained for attribution",
        ],
    }
