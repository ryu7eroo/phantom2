import pytest

from ctf_swarm.model_worker import build_prompt, parse_response
from ctf_swarm.openai_compatible_provider import OpenAICompatibleProvider


def test_parse_candidate_flag():
    event = parse_response("I found LISA{flag-123} after analysis.", "w1", "t1", "divergent")
    assert event["type"] == "candidate"
    assert event["value"] == "LISA{flag-123}"
    assert event["task_id"] == "t1"


def test_parse_evidence():
    event = parse_response("No flag yet; UAF looks promising.", "w1", "t1", "independent")
    assert event["type"] == "evidence"
    assert event["round"] == "independent"
    assert event["worker_id"] == "w1"


def test_provider_defaults_to_openai_compatible_endpoint():
    provider = OpenAICompatibleProvider("test-model", base_url="http://example/v1")
    assert provider.base_url == "http://example/v1"
    assert provider.model == "test-model"


@pytest.mark.parametrize("round_name", ["independent", "collaborative", "divergent", "adversarial"])
def test_parse_evidence_round(round_name):
    event = parse_response("evidence", "w", "t", round_name)
    assert event["round"] == round_name


def test_build_prompt_uses_no_think_for_fast_rounds():
    prompt = build_prompt(
        {"round": "independent", "title": "demo", "category": "misc", "points": "100"},
        {"history": [], "dead_ends": [], "open_hypotheses": []},
    )
    assert "Use /no_think" in prompt
    assert "Avoid repeating known dead ends" in prompt


def test_build_prompt_uses_think_for_divergent_round():
    prompt = build_prompt(
        {"round": "divergent", "title": "demo", "category": "misc", "points": "100"},
        {"history": [{"claim": "known path"}], "dead_ends": ["old"], "open_hypotheses": ["new"]},
    )
    assert "known path" in prompt
    assert "old" in prompt
    assert "new" in prompt
    assert "Use /think" in prompt


def test_build_prompt_uses_adversarial_role():
    prompt = build_prompt(
        {"round": "adversarial", "title": "demo", "category": "misc", "points": "100"},
        {"history": [{"claim": "known path"}], "dead_ends": ["old"], "open_hypotheses": ["new"]},
    )
    assert "adversarial" in prompt.lower()
    assert "falsify" in prompt.lower() or "challenge" in prompt.lower()
    assert "Use /think" in prompt
