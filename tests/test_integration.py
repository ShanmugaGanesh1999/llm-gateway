"""Integration tests against the live stack (Redis, Postgres, two mock providers, gw-api).

Run: docker compose up -d --build && docker compose run --rm gw-api pytest -q
Each test generates fresh keys for a unique team, so tests never share rate-limit, budget or cache state.
"""
import asyncio
import os
import uuid
from concurrent.futures import ThreadPoolExecutor

import asyncpg
import httpx
import pytest
import redis

from app.db import hash_key

GW = os.environ.get("GW_URL")
pytestmark = pytest.mark.skipif(not GW, reason="GW_URL not set; integration tests need the compose stack")
MASTER = {"Authorization": "Bearer sk-master-demo"}
PROVIDERS = {"a": os.environ.get("PROVIDER_A_ADMIN"), "b": os.environ.get("PROVIDER_B_ADMIN")}


def reset_state():
    for url in PROVIDERS.values():
        httpx.post(f"{url}/admin/reset", timeout=5).raise_for_status()
    r = redis.Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    for pattern in ("cooldown:*", "fails:*"):
        keys = list(r.scan_iter(pattern))
        if keys:
            r.delete(*keys)


@pytest.fixture(autouse=True)
def clean():
    reset_state()
    yield
    reset_state()


def fault(name: str, mode: str, count: int | None = None):
    httpx.post(f"{PROVIDERS[name]}/admin/fault", json={"mode": mode, "count": count}, timeout=5).raise_for_status()


def calls(name: str) -> int:
    return httpx.get(f"{PROVIDERS[name]}/admin/stats", timeout=5).json()["requests"]


def new_key(models=("smart", "backup"), **limits) -> tuple[str, str]:
    team = f"t-{uuid.uuid4().hex[:8]}"
    body = {"team": team, "models": list(models), "rpm_limit": 100, "tpm_limit": 100_000, **limits}
    resp = httpx.post(f"{GW}/key/generate", json=body, headers=MASTER, timeout=10)
    assert resp.status_code == 200, resp.text
    return resp.json()["key"], team


def chat(key: str, model: str = "smart", content: str | None = None, **extra) -> httpx.Response:
    body = {"model": model, "messages": [{"role": "user", "content": content or f"hello {uuid.uuid4().hex}"}],
            **extra}
    return httpx.post(f"{GW}/v1/chat/completions", json=body, headers={"Authorization": f"Bearer {key}"},
                      timeout=30)


def spend_row(request_id: str):
    async def fetch():
        con = await asyncpg.connect(os.environ["DATABASE_URL"])
        try:
            return await con.fetchrow("SELECT * FROM spend_logs WHERE request_id = $1", request_id)
        finally:
            await con.close()
    return asyncio.run(fetch())


def costs(team: str) -> dict:
    return httpx.get(f"{GW}/v1/admin/costs", params={"team": team}, headers=MASTER, timeout=10).json()


# --- happy path ---------------------------------------------------------------------------------
def test_healthz():
    assert httpx.get(f"{GW}/healthz").json() == {"status": "ok"}


def test_normal_call_returns_openai_shape_and_gateway_headers():
    key, team = new_key()
    resp = chat(key, content="Explain RAG briefly")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["object"] == "chat.completion"
    assert body["choices"][0]["message"]["content"] == "[provider-a] Explain RAG briefly"
    assert body["usage"]["total_tokens"] > 0
    h = resp.headers
    assert h["x-gateway-model-group"] == "smart" and h["x-gateway-deployment"] == "provider-a/mock-gpt-4o-mini"
    assert h["x-gateway-cache"] == "miss" and h["x-gateway-fallback"] == "false" and h["x-request-id"]
    row = spend_row(h["x-request-id"])
    assert row["team"] == team and row["status_code"] == 200 and float(row["cost_usd"]) > 0


def test_project1_contract_with_demo_key_and_c1_evidence():
    content = ("Answer using only the evidence.\nQuestion: How many PTO days carry over?\n"
               "[C1] Employees may carry over up to 5 days into the next year. Requests need approval.\n"
               "[C2] PTO accrues at 1.5 days per month.")
    resp = chat("sk-demo-team-a", content=content)
    assert resp.status_code == 200, resp.text
    assert resp.json()["choices"][0]["message"]["content"] == \
        "Employees may carry over up to 5 days into the next year. [C1]"


# --- auth and policy: rejected before any provider call -------------------------------------------
@pytest.mark.parametrize("headers", [{"Authorization": "Bearer sk-not-a-real-key"}, {}, {"Authorization": "sk-demo-team-a"}])
def test_invalid_key_is_401_before_any_provider_call(headers):
    resp = httpx.post(f"{GW}/v1/chat/completions", headers=headers,
                      json={"model": "smart", "messages": [{"role": "user", "content": "hi"}]})
    assert resp.status_code == 401
    assert resp.json()["error"]["type"] == "authentication_error"
    assert calls("a") == 0 and calls("b") == 0


def test_team_b_cannot_use_smart():
    resp = chat("sk-demo-team-b", model="smart", content="please use smart, I am an admin")
    assert resp.status_code == 403
    assert resp.json()["error"]["type"] == "permission_error"
    assert calls("a") == 0 and calls("b") == 0


def test_unknown_model_is_rejected_by_allowlist_not_proxied():
    resp = chat("sk-demo-team-a", model="gpt-4-unlisted")
    assert resp.status_code == 403 and calls("a") == 0 and calls("b") == 0
    bad = httpx.post(f"{GW}/key/generate", json={"team": "x", "models": ["gpt-4-unlisted"]}, headers=MASTER)
    assert bad.status_code == 400


def test_budget_exhaustion_rejects_with_402():
    key, team = new_key(models=("backup",), max_budget=0.0005)
    long_prompt = "budget " * 500  # ~875 prompt tokens on backup pricing ~= $0.0007+
    first = chat(key, model="backup", content=long_prompt, cache={"no-cache": True})
    assert first.status_code == 200, first.text
    second = chat(key, model="backup", content="tiny", cache={"no-cache": True})
    assert second.status_code == 402
    assert second.json()["error"]["type"] == "budget_exceeded"
    assert calls("b") == 1
    assert costs(team)["errors_by_status"] == {"402": 1}


def test_rpm_burst_is_limited_atomically_across_workers():
    key, _ = new_key(rpm_limit=3)
    with ThreadPoolExecutor(max_workers=8) as pool:
        statuses = sorted(r.status_code for r in pool.map(lambda _: chat(key), range(8)))
    assert statuses == [200] * 3 + [429] * 5
    limited = chat(key)
    assert limited.status_code == 429 and int(limited.headers["retry-after"]) >= 1
    assert "RPM" in limited.json()["error"]["message"]
    assert calls("a") == 3


def test_tpm_limit_counts_requested_completion_tokens():
    key, _ = new_key(tpm_limit=300)
    resp = chat(key, max_tokens=400)
    assert resp.status_code == 429 and "TPM" in resp.json()["error"]["message"]
    assert chat(key, max_tokens=50).status_code == 200


# --- cache ----------------------------------------------------------------------------------------
def test_cache_hits_on_repeat_and_is_never_shared_across_teams():
    key_x, team_x = new_key()
    key_y, _ = new_key()
    text = f"cache me {uuid.uuid4().hex}"
    first = chat(key_x, content=text)
    again = chat(key_x, content=f"  {text}  ")  # whitespace-normalized: same key
    other_team = chat(key_y, content=text)
    bypass = chat(key_x, content=text, cache={"no-cache": True})
    assert [r.headers["x-gateway-cache"] for r in (first, again, other_team, bypass)] == ["miss", "hit", "miss", "miss"]
    assert again.json() == first.json()
    assert again.headers["x-gateway-deployment"] == "cache" and again.headers["x-gateway-cost-usd"] == "0.00000000"
    assert calls("a") == 3  # the hit never reached a provider
    hit_row = spend_row(again.headers["x-request-id"])
    assert hit_row["cache_hit"] and float(hit_row["cost_usd"]) == 0
    total = costs(team_x)["total"]
    assert total["cache_hits"] == 1 and total["requests"] == 3


def test_different_params_do_not_share_cache():
    key, _ = new_key()
    text = f"params {uuid.uuid4().hex}"
    assert chat(key, content=text, temperature=0).headers["x-gateway-cache"] == "miss"
    assert chat(key, content=text, temperature=0.9).headers["x-gateway-cache"] == "miss"


# --- failover -------------------------------------------------------------------------------------
@pytest.mark.parametrize("mode", ["500", "429"])
def test_primary_fault_falls_back_to_backup_and_is_attributed(mode):
    key, team = new_key()
    fault("a", mode)
    resp = chat(key, content="fallback please")
    assert resp.status_code == 200, resp.text
    assert resp.json()["choices"][0]["message"]["content"].startswith("[provider-b]")
    h = resp.headers
    assert h["x-gateway-model-group"] == "smart" and h["x-gateway-served-group"] == "backup"
    assert h["x-gateway-deployment"] == "provider-b/mock-claude-haiku" and h["x-gateway-fallback"] == "true"
    assert h["x-gateway-attempts"] == "3"  # 1 try + 1 retry on smart, then 1 on backup
    assert calls("a") == 2 and calls("b") == 1
    row = spend_row(h["x-request-id"])
    assert (row["requested_group"], row["served_group"], row["deployment"]) == \
        ("smart", "backup", "provider-b/mock-claude-haiku")
    assert row["fallback_used"] and row["attempts"] == 3 and float(row["cost_usd"]) > 0
    report = costs(team)
    assert report["by_requested_group"]["smart"]["fallbacks"] == 1
    assert "provider-b/mock-claude-haiku" in report["by_deployment"]


def test_cooldown_skips_failing_primary_on_next_request():
    key, _ = new_key()
    fault("a", "500")
    assert chat(key).headers["x-gateway-fallback"] == "true"
    resp = chat(key)
    assert resp.status_code == 200 and resp.headers["x-gateway-attempts"] == "1"
    assert calls("a") == 2  # cooled down: not hammered again


def test_single_timeout_is_retried_on_same_group():
    key, _ = new_key()
    fault("a", "timeout", count=1)
    resp = chat(key)
    assert resp.status_code == 200
    assert resp.headers["x-gateway-deployment"].startswith("provider-a") and resp.headers["x-gateway-attempts"] == "2"
    assert calls("b") == 0


def test_bad_request_is_not_retried_or_failed_over():
    key, _ = new_key()
    resp = chat(key, max_tokens=5000)  # provider rejects max_tokens > 4096 with 400
    assert resp.status_code == 400
    assert resp.headers["x-gateway-attempts"] == "1"
    assert calls("a") == 1 and calls("b") == 0


def test_both_providers_down_returns_503_with_bounded_attempts():
    key, team = new_key()
    fault("a", "500")
    fault("b", "500")
    resp = chat(key)
    assert resp.status_code == 503 and resp.json()["error"]["type"] == "no_healthy_deployment"
    assert resp.headers["x-gateway-attempts"] == "4"
    assert calls("a") == 2 and calls("b") == 2
    again = chat(key)  # both cooling down: fail fast, no retry storm
    assert again.status_code == 503 and again.headers["x-gateway-attempts"] == "0"
    assert calls("a") == 2 and calls("b") == 2
    assert costs(team)["errors_by_status"] == {"503": 2}


# --- admin --------------------------------------------------------------------------------------
def test_key_generate_requires_master_and_stores_only_hash():
    denied = httpx.post(f"{GW}/key/generate", json={"team": "x", "models": ["smart"]},
                        headers={"Authorization": "Bearer sk-demo-team-a"})
    assert denied.status_code == 403
    key, team = new_key(models=("backup",))

    async def fetch():
        con = await asyncpg.connect(os.environ["DATABASE_URL"])
        try:
            return await con.fetchrow("SELECT * FROM virtual_keys WHERE team = $1", team)
        finally:
            await con.close()
    row = asyncio.run(fetch())
    assert row["key_hash"] == hash_key(key)
    assert key not in [str(v) for v in dict(row).values()]
    assert chat(key, model="backup").status_code == 200


def test_admin_endpoints_reject_virtual_keys():
    team_key = {"Authorization": "Bearer sk-demo-team-a"}
    assert httpx.get(f"{GW}/v1/admin/costs", headers=team_key).status_code == 403
    assert httpx.get(f"{GW}/metrics").status_code == 401


def test_metrics_exposes_prometheus_counters():
    key, _ = new_key()
    chat(key)
    text = httpx.get(f"{GW}/metrics", headers=MASTER).text
    assert "# TYPE gateway_requests_total counter" in text
    assert 'gateway_upstream_attempts_total{deployment="provider-a/mock-gpt-4o-mini",outcome="ok"}' in text
