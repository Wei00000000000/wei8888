from backend.app.quant.universe_agent import universe_research
from backend.app.quant.engine import QuantConfig


def test_empty_universe_keeps_champion():
    out = universe_research({})
    assert out["decision"] == "keep_champion"
    assert out["promotion_candidate"] is None
    assert out["production_mutation"] is False


def test_candidate_key_space_uses_shared_config():
    # The universe agent deliberately accepts one champion config for all symbols.
    cfg = QuantConfig(volume_multiplier=1.3)
    out = universe_research({}, cfg)
    assert out["champion_config"]["volume_multiplier"] == 1.3
    assert "One shared strategy across symbols; no per-coin curve fitting" in out["guardrails"]
