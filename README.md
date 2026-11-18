# Project 2: LLM Gateway (caching, failover, rate limits, cost tracking)

An OpenAI-compatible gateway that sits between applications and model providers. Apps call one
endpoint with a **virtual key** and a **logical model group** (`smart`); the gateway decides which
physical deployment answers, and enforces who may call what, how often, and for how much money.

It is a small FastAPI service (~1,000 lines of Python) that mimics LiteLLM Proxy's behaviour and
config shape. Redis and PostgreSQL are real; the two providers are a deterministic mock with fault
injection, so the whole thing runs offline with no paid keys.

```text
app ──► gw-api ──► auth (Postgres, SHA-256 key hash) ──► allowlist ──► RPM/TPM (Redis Lua)
                 ──► budget (Postgres spend + Redis reservations) ──► tenant-scoped cache (Redis)
                 ──► router: shuffle / least-busy, retry transient only, cooldown, smart → backup
                 ──► gw-provider-a / gw-provider-b ──► spend row (Postgres) + metrics
```

## Quickstart

```bash
docker compose up -d --build
curl -s localhost:8102/healthz                      # {"status":"ok"}
scripts/demo.sh                                     # full walkthrough (needs curl + python3)
docker compose run --rm gw-api pytest -q            # unit + integration tests against the live stack
docker compose down -v
```

Swagger UI: http://127.0.0.1:8102/docs. Mock providers: `127.0.0.1:8112` (a), `127.0.0.1:8113` (b).

## Demo keys (CONVENTIONS section 5)

| Key | Who | Models | Budget | Limits |
|---|---|---|---|---|
| `sk-master-demo` | admin | key generation, cost reports, metrics | | |
| `sk-demo-team-a` | team `team-a` | `smart`, `backup` | $5.00 / 30d | 20 RPM, 12,000 TPM |
| `sk-demo-rag-service` | team `rag-service` | `smart`, `backup` | $5.00 / 30d | 300 RPM, 200,000 TPM (Project 1 in the combined stack) |
| `sk-demo-team-b` | team `team-b` | `backup` only | $0.0005 / 30d | 5 RPM, 4,000 TPM |

Keys are seeded on startup from `data/seed_keys.yaml` and stored only as SHA-256 hashes.

## Walkthrough (curl)

```bash
# Normal call. Look at the x-gateway-* headers.
curl -si localhost:8102/v1/chat/completions -H 'Authorization: Bearer sk-demo-team-a' \
  -H 'Content-Type: application/json' \
  -d '{"model":"smart","messages":[{"role":"user","content":"Explain RAG in one line"}]}'
#   x-gateway-model-group: smart        x-gateway-deployment: provider-a/mock-gpt-4o-mini
#   x-gateway-cache: miss               x-gateway-fallback: false     x-request-id: req-...

# Repeat it: x-gateway-cache: hit, x-gateway-cost-usd: 0.00000000
# Bypass the cache: add "cache": {"no-cache": true}   (and "no-store": true to skip writing)

# Break provider-a, then call again: served by backup, attempts=3, cost attributed to provider-b
curl -s -X POST localhost:8112/admin/fault -H 'Content-Type: application/json' -d '{"mode":"500"}'
curl -s -X POST localhost:8112/admin/fault -H 'Content-Type: application/json' -d '{"mode":"ok"}'
# modes: ok | 429 | 500 | timeout ; "count": N limits the fault to N requests

# Rejections
#   bad key            -> 401 authentication_error (no provider call)
#   team-b + "smart"   -> 403 permission_error
#   team-b over budget -> 402 budget_exceeded
#   over RPM/TPM       -> 429 rate_limit_error + Retry-After
#   provider 400       -> 400 returned as-is, never retried or failed over
#   everything down    -> 503 no_healthy_deployment after a bounded number of attempts

# Admin (master key only)
curl -s -X POST localhost:8102/key/generate -H 'Authorization: Bearer sk-master-demo' \
  -H 'Content-Type: application/json' \
  -d '{"team":"search","models":["smart"],"max_budget":5,"budget_duration":"30d","rpm_limit":20,"tpm_limit":12000}'
curl -s 'localhost:8102/v1/admin/costs?team=team-a' -H 'Authorization: Bearer sk-master-demo'
curl -s localhost:8102/metrics -H 'Authorization: Bearer sk-master-demo'
```

## Using it from other projects

Project 1 (`RAG_LLM_MODE=gateway`) calls `http://gw-api:8000/v1/chat/completions` with
`Authorization: Bearer sk-demo-team-a` (or `sk-demo-rag-service` in the combined stack) and
`model: "smart"`. When the last user message contains `[C1] ...` evidence lines, the mock provider
answers with the one or two evidence sentences that share the most words with the `Question:` line,
each followed by its citation, for example `... [C1]`. With no question line it returns the first
full sentence of `[C1]`. Any OpenAI SDK works with `base_url="http://127.0.0.1:8102/v1"`.

## Optional real upstream

Off by default. When both `REAL_UPSTREAM_BASE_URL` (e.g. `https://api.openai.com/v1`) and
`REAL_UPSTREAM_API_KEY` are set, the `smart` group is routed to that OpenAI-compatible endpoint
(model `REAL_UPSTREAM_MODEL`, default `gpt-4o-mini`); `backup` stays on mock provider-b, so the
fallback path still works. Set `REAL_UPSTREAM_INPUT_COST` / `REAL_UPSTREAM_OUTPUT_COST` (USD per
token) for correct cost rows. This sends real traffic and costs real money.

## Mapping to real LiteLLM Proxy

`config/gateway.yaml` uses LiteLLM's `model_list` / `router_settings` / `litellm_settings` /
`general_settings` shape and `os.environ/NAME` references, and is what this gateway loads.
`config/litellm_config.reference.yaml` is the equivalent real LiteLLM config (not run here). The
endpoints `/v1/chat/completions` and `/key/generate` take the same fields as LiteLLM's
(`models`, `max_budget`, `budget_duration`, `rpm_limit`, `tpm_limit`, `cache: {"no-cache": true}`).
See `docs/system-design.md` section 8 for what differs.

## Folder map

```text
app/main.py           FastAPI app: request lifecycle, admin endpoints, error mapping
app/config.py         loads config/gateway.yaml, price math, real-upstream override
app/db.py             Postgres: hashed virtual keys, spend logs, cost report
app/limits.py         Redis Lua: sliding-window RPM/TPM, in-flight budget reservations
app/cache.py          tenant-scoped response cache key + fail-open Redis cache
app/router.py         deployment pick, transient-error classification, retries, cooldown, fallback
app/metrics.py        Prometheus counters kept in a Redis hash (shared across workers)
mock_provider/main.py deterministic OpenAI-compatible provider with /admin/fault
config/               gateway.yaml (loaded) and litellm_config.reference.yaml (reference)
data/                 schema.sql, seed_keys.yaml
docs/system-design.md design doc (Hello Interview format)
scripts/demo.sh       end-to-end walkthrough
tests/                test_unit.py (no services), test_integration.py (live stack)
```
