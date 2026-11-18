"""Unit tests: no running services needed."""
from pathlib import Path

import httpx
import pytest
import yaml

from app.cache import cache_key
from app.config import Deployment, cost_usd, parse_duration, parse_settings
from app.db import hash_key, summarize
from app.limits import window_position
from app.router import is_transient
from mock_provider.main import reply_for

ROOT = Path(__file__).resolve().parent.parent
ENV = {"PROVIDER_A_URL": "http://a/v1", "PROVIDER_A_KEY": "ka", "PROVIDER_B_URL": "http://b/v1",
       "PROVIDER_B_KEY": "kb", "GATEWAY_MASTER_KEY": "sk-master-demo"}
MSGS = [{"role": "user", "content": "What is RAG?"}]


def raw_config():
    return yaml.safe_load((ROOT / "config" / "gateway.yaml").read_text())


# --- cache key isolation -------------------------------------------------------------------
def test_cache_key_is_deterministic_and_normalizes_whitespace():
    a = cache_key("team-a", "smart", MSGS, {"temperature": 0}, "v1")
    b = cache_key("team-a", "smart", [{"role": "user", "content": "  What is RAG?\r\n"}], {"temperature": 0}, "v1")
    assert a == b and a.startswith("cache:team-a:smart:")


@pytest.mark.parametrize("change", [
    {"team": "team-b"}, {"group": "backup"}, {"policy": "v2"},
    {"params": {"temperature": 0.7}}, {"params": {"temperature": 0, "max_tokens": 10}},
    {"messages": [{"role": "system", "content": "be brief"}, *MSGS]},
])
def test_cache_key_changes_with_every_scope_dimension(change):
    base = dict(team="team-a", group="smart", messages=MSGS, params={"temperature": 0}, policy="v1")
    changed = {**base, **change}
    assert cache_key(*base.values()) != cache_key(*changed.values())


def test_cache_key_ignores_non_semantic_fields():
    assert cache_key("t", "smart", MSGS, {"user": "u1", "cache": {"no-cache": True}}, "v1") == \
        cache_key("t", "smart", MSGS, {}, "v1")


# --- cost math -------------------------------------------------------------------------------
def test_cost_math_uses_separate_input_and_output_prices():
    dep = Deployment("d", "smart", "openai", "m", "http://x", "k", 1, 1, 0.00000015, 0.0000006)
    assert cost_usd(dep, 1_000_000, 0) == pytest.approx(0.15)
    assert cost_usd(dep, 1000, 500) == pytest.approx(1000 * 0.15e-6 + 500 * 0.6e-6)
    assert cost_usd(dep, 0, 0) == 0


def test_configured_prices_make_backup_more_expensive():
    s = parse_settings(raw_config(), ENV)
    smart, backup = s.deployments["smart"][0], s.deployments["backup"][0]
    assert cost_usd(backup, 100, 100) > cost_usd(smart, 100, 100) > 0


# --- retry classification --------------------------------------------------------------------
@pytest.mark.parametrize("status", [408, 429, 500, 502, 503, 504, 529])
def test_transient_statuses_are_retried(status):
    assert is_transient(status)


@pytest.mark.parametrize("status", [400, 401, 403, 404, 413, 422])
def test_client_errors_are_not_retried(status):
    assert not is_transient(status)


def test_transport_errors_are_transient_but_bugs_are_not():
    assert is_transient(exc=httpx.ReadTimeout("slow"))
    assert is_transient(exc=httpx.ConnectError("refused"))
    assert not is_transient(exc=ValueError("bad json"))


# --- config parsing --------------------------------------------------------------------------
def test_gateway_yaml_parses_into_groups_fallbacks_and_policy():
    s = parse_settings(raw_config(), ENV)
    assert s.groups == {"smart", "backup"}
    assert s.fallbacks == {"smart": ["backup"]}
    assert s.deployments["smart"][0].id == "provider-a/mock-gpt-4o-mini"
    assert s.deployments["smart"][0].model == "mock-gpt-4o-mini"
    assert s.deployments["smart"][0].api_base == "http://a/v1"
    assert s.master_key == "sk-master-demo" and s.cache_enabled and s.cache_ttl == 300
    assert s.num_retries == 1 and s.routing_strategy == "simple-shuffle"


def test_unset_env_reference_fails_fast():
    env = {k: v for k, v in ENV.items() if k != "PROVIDER_A_KEY"}
    with pytest.raises(ValueError, match="PROVIDER_A_KEY"):
        parse_settings(raw_config(), env)


@pytest.mark.parametrize("fallbacks", [[{"smart": ["missing"]}], [{"smart": ["smart"]}]])
def test_invalid_fallbacks_are_rejected(fallbacks):
    raw = raw_config()
    raw["router_settings"]["fallbacks"] = fallbacks
    with pytest.raises(ValueError, match="invalid fallback"):
        parse_settings(raw, ENV)


def test_unknown_routing_strategy_is_rejected():
    raw = raw_config()
    raw["router_settings"]["routing_strategy"] = "round-robin-ish"
    with pytest.raises(ValueError, match="routing_strategy"):
        parse_settings(raw, ENV)


def test_real_upstream_replaces_smart_only_when_both_vars_set():
    s = parse_settings(raw_config(), {**ENV, "REAL_UPSTREAM_BASE_URL": "https://api.example/v1"})
    assert s.deployments["smart"][0].id.startswith("provider-a")
    s = parse_settings(raw_config(), {**ENV, "REAL_UPSTREAM_BASE_URL": "https://api.example/v1",
                                      "REAL_UPSTREAM_API_KEY": "sk-real"})
    assert s.deployments["smart"][0].id == "real/gpt-4o-mini"
    assert s.deployments["backup"][0].id.startswith("provider-b")


@pytest.mark.parametrize("text,seconds", [("30d", 2_592_000), ("24h", 86_400), ("15m", 900), ("90s", 90)])
def test_parse_duration(text, seconds):
    assert parse_duration(text) == seconds


def test_parse_duration_rejects_garbage():
    with pytest.raises(ValueError):
        parse_duration("1 month")


# --- rate window, hashing, reporting, mock provider ---------------------------------------------
def test_sliding_window_weight_decays_across_the_minute():
    assert window_position(600.0) == (10, 1.0)
    assert window_position(630.0) == (10, 0.5)
    bucket, weight = window_position(659.0)
    assert bucket == 10 and weight == pytest.approx(1 / 60)


def test_keys_are_stored_as_sha256():
    assert hash_key("sk-demo-team-a") == \
        "%064x" % int.from_bytes(__import__("hashlib").sha256(b"sk-demo-team-a").digest(), "big")
    assert len(hash_key("x")) == 64


def test_cost_summary_rates():
    row = dict(team="t", requested_group="smart", served_group="backup", deployment="provider-b/x",
               status_code=200, cache_hit=False, fallback_used=True, requests=1, cost=0.002, pt=10, ct=10,
               attempts=3)
    rows = [row, {**row, "deployment": "cache", "cache_hit": True, "fallback_used": False, "cost": 0, "attempts": 0},
            {**row, "status_code": 503, "fallback_used": False, "cost": 0}]
    report = summarize(rows, "t", 30)
    total = report["total"]
    assert total["requests"] == 3 and total["successful"] == 2
    assert total["cache_hit_rate"] == 0.5 and total["fallback_rate"] == 1.0
    assert report["errors_by_status"] == {"503": 1}
    assert report["by_deployment"]["provider-b/x"]["cost_usd"] == pytest.approx(0.002)


def test_mock_provider_answers_from_c1_evidence():
    msg = [{"role": "user", "content": "Question?\n[C1] Carry over up to 5 days. Approval needed.\n[C2] Other."}]
    assert reply_for(msg) == "Carry over up to 5 days. [C1]"
    assert reply_for([{"role": "user", "content": "hello"}]).endswith("] hello")
