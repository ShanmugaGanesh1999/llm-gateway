"""FastAPI LLM gateway: auth -> allowlist -> RPM/TPM -> budget -> cache -> route -> spend."""
import hmac
import json
import logging
import os
import secrets
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any, Literal

import asyncpg
import httpx
import redis.asyncio as aioredis
from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from .cache import ResponseCache, cache_key
from .config import cost_usd, load_settings, parse_duration
from .db import DATABASE_URL, SpendRecord, Store, VirtualKey, hash_key
from .limits import Limiter
from .metrics import Metrics
from .router import RouteFailed, Router

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("gateway")
DEFAULT_COMPLETION_ESTIMATE = 256  # tokens assumed when max_tokens is absent (TPM + budget estimate)


class Message(BaseModel):
    model_config = ConfigDict(extra="allow")
    role: Literal["system", "user", "assistant", "tool", "developer"]
    content: str | list[dict[str, Any]] | None = None


class ChatRequest(BaseModel):
    model_config = ConfigDict(extra="allow")  # other OpenAI params pass through and enter the cache key
    model: str
    messages: list[Message] = Field(min_length=1)
    max_tokens: int | None = Field(default=None, ge=1)
    temperature: float | None = None
    stream: bool = False
    cache: dict[str, Any] | None = None  # LiteLLM-style {"no-cache": true, "no-store": true}


class KeyRequest(BaseModel):
    team: str = Field(min_length=1, max_length=64, pattern=r"^[a-z0-9][a-z0-9_-]*$")
    models: list[str] = Field(min_length=1)
    key_alias: str | None = None
    max_budget: float | None = Field(default=None, ge=0)
    budget_duration: str = "30d"
    rpm_limit: int | None = Field(default=None, ge=1)
    tpm_limit: int | None = Field(default=None, ge=1)


class GatewayError(Exception):
    def __init__(self, status: int, etype: str, message: str, headers: dict | None = None):
        super().__init__(message)
        self.status, self.etype, self.message, self.headers = status, etype, message, headers or {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = load_settings()
    redis = aioredis.from_url(os.environ.get("REDIS_URL", "redis://localhost:6379/0"), decode_responses=True)
    store = await Store.connect(DATABASE_URL)
    await store.init()
    http = httpx.AsyncClient(limits=httpx.Limits(max_connections=200))
    metrics = Metrics(redis)
    state = dict(
        settings=settings, redis=redis, store=store, http=http, metrics=metrics, auth_cache={},
        limiter=Limiter(redis), cache=ResponseCache(redis, settings.cache_ttl, settings.cache_enabled),
        router=Router(settings, redis, http, metrics))
    for name, value in state.items():
        setattr(app.state, name, value)
    log.info("gateway ready: groups=%s strategy=%s", sorted(settings.groups), settings.routing_strategy)
    yield
    await http.aclose()
    await store.pool.close()
    await redis.aclose()


app = FastAPI(title="LLM Gateway (P2)", version="1.0.0", lifespan=lifespan)


def error_response(status: int, etype: str, message: str, request_id: str, headers: dict | None = None):
    body = {"error": {"message": message, "type": etype, "code": str(status)}}
    return JSONResponse(body, status_code=status, headers={"x-request-id": request_id, **(headers or {})})


@app.exception_handler(GatewayError)
async def gateway_error(request: Request, exc: GatewayError):
    rid = getattr(request.state, "request_id", str(uuid.uuid4()))
    return error_response(exc.status, exc.etype, exc.message, rid, exc.headers)


@app.exception_handler(aioredis.RedisError)
@app.exception_handler(asyncpg.PostgresError)
@app.exception_handler(OSError)
async def dependency_down(request: Request, exc: Exception):
    """Fail closed: no limits or budget checks means no upstream spend."""
    log.error("dependency failure: %r", exc)
    rid = getattr(request.state, "request_id", str(uuid.uuid4()))
    return error_response(503, "dependency_unavailable", "gateway state store unavailable", rid)


def bearer(request: Request) -> str:
    header = request.headers.get("authorization", "")
    scheme, _, token = header.partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        raise GatewayError(401, "authentication_error", "missing bearer token")
    return token.strip()


async def authenticate(request: Request) -> VirtualKey:
    """SHA-256 lookup in Postgres, cached per worker for auth_cache_ttl seconds (revocation lag)."""
    digest = hash_key(bearer(request))
    st = request.app.state
    hit = st.auth_cache.get(digest)
    if hit and hit[0] > time.monotonic():
        return hit[1]
    key = await st.store.get_key(digest)
    if key is None:
        raise GatewayError(401, "authentication_error", "invalid or revoked API key")
    st.auth_cache[digest] = (time.monotonic() + st.settings.auth_cache_ttl, key)
    return key


def require_master(request: Request) -> None:
    if not hmac.compare_digest(bearer(request), request.app.state.settings.master_key):
        raise GatewayError(403, "permission_error", "master key required")


def _chars(messages: list[Message]) -> int:
    return sum(len(m.content if isinstance(m.content, str) else json.dumps(m.content or "")) for m in messages)


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


@app.post("/v1/chat/completions")
async def chat_completions(request: Request):
    st = request.app.state
    rid = request.headers.get("x-request-id") or f"req-{uuid.uuid4().hex[:16]}"
    request.state.request_id = rid
    started = time.monotonic()
    key = await authenticate(request)  # 1. before parsing the body or touching any provider

    try:
        body = ChatRequest.model_validate(await request.json())
    except (ValidationError, json.JSONDecodeError, ValueError) as exc:
        raise GatewayError(400, "invalid_request_error", f"invalid request body: {str(exc)[:300]}")
    if body.stream:
        raise GatewayError(400, "invalid_request_error", "stream=true is not supported by this gateway")

    group = body.model
    rec = SpendRecord(request_id=rid, key=key, requested_group=group)
    reserved = 0.0
    try:
        # 2. Model allowlist (checked before revealing whether the group exists).
        if group not in key.models:
            raise GatewayError(403, "permission_error", f"key for team '{key.team}' may not use model '{group}'")
        if group not in st.settings.groups:
            raise GatewayError(400, "invalid_request_error", f"model group '{group}' is not configured")

        # 3. Per-key RPM/TPM (atomic Lua sliding window in Redis, shared by every worker).
        prompt_est = max(1, _chars(body.messages) // 4)
        completion_est = body.max_tokens or DEFAULT_COMPLETION_ESTIMATE
        rate = await st.limiter.check_rate(key.key_id, key.rpm_limit, key.tpm_limit, prompt_est + completion_est)
        if not rate.allowed:
            raise GatewayError(429, "rate_limit_error", f"{rate.reason.upper()} limit exceeded for this key",
                               {"Retry-After": str(rate.retry_after)})

        # 4. Budget: durable spend (Postgres) + in-flight reservations (Redis) must stay under the cap.
        if key.max_budget is not None:
            spent = await st.store.spent(key)
            candidates = [d for g in [group, *st.settings.fallbacks.get(group, [])]
                          for d in st.settings.deployments[g]]
            reserved = max(cost_usd(d, prompt_est, completion_est) for d in candidates)
            if not await st.limiter.reserve_budget(key.key_id, spent, key.max_budget, reserved):
                reserved = 0.0
                raise GatewayError(402, "budget_exceeded",
                                   f"budget exhausted: spent ${spent:.6f} of ${key.max_budget:.6f}")

        # 5. Tenant-scoped response cache.
        opts = body.cache or {}
        params = body.model_dump(exclude={"model", "messages", "cache", "stream"}, exclude_none=True)
        messages = [m.model_dump(exclude_none=True) for m in body.messages]
        ckey = cache_key(key.team, group, messages, params, st.settings.policy_version)
        if not opts.get("no-cache"):
            entry = await st.cache.get(ckey)
            if entry:
                await st.metrics.inc("gateway_cache_total", result="hit")
                usage = entry["response"].get("usage") or {}
                await st.limiter.adjust_tokens(key.key_id, rate.bucket, -(prompt_est + completion_est))
                rec.__dict__.update(served_group=entry["served_group"], cache_hit=True, status_code=200,
                                    prompt_tokens=0, completion_tokens=0)
                return _ok(entry["response"], rid, group, entry["served_group"], "cache", "hit", False, 0, 0.0,
                           usage)
        await st.metrics.inc("gateway_cache_total", result="bypass" if opts.get("no-cache") else "miss")

        # 6. Route with bounded retries, cooldown and fallback.
        try:
            result = await st.router.route(group, {"messages": messages, **params})
        except RouteFailed as exc:
            rec.attempts = exc.attempts
            if exc.deployment:
                rec.deployment, rec.served_group = exc.deployment.id, exc.group
            raise GatewayError(exc.status, exc.etype, exc.message, {"x-gateway-attempts": str(exc.attempts)})

        usage = result.response.get("usage") or {}
        pt, ct = int(usage.get("prompt_tokens", 0)), int(usage.get("completion_tokens", 0))
        cost = cost_usd(result.deployment, pt, ct)
        await st.limiter.adjust_tokens(key.key_id, rate.bucket, (pt + ct) - (prompt_est + completion_est))
        rec.__dict__.update(served_group=result.served_group, deployment=result.deployment.id,
                            provider_model=result.deployment.model, prompt_tokens=pt, completion_tokens=ct,
                            cost_usd=cost, fallback_used=result.fallback_used, attempts=result.attempts,
                            status_code=200)
        if not opts.get("no-store"):
            await st.cache.set(ckey, {"response": result.response, "served_group": result.served_group,
                                      "deployment": result.deployment.id})
        if result.fallback_used:
            await st.metrics.inc("gateway_fallbacks_total", requested_group=group, served_group=result.served_group)
        await st.metrics.inc("gateway_cost_usd_total", float(cost), team=key.team)
        await st.metrics.inc("gateway_tokens_total", pt, team=key.team, kind="prompt")
        await st.metrics.inc("gateway_tokens_total", ct, team=key.team, kind="completion")
        return _ok(result.response, rid, group, result.served_group, result.deployment.id, "miss",
                   result.fallback_used, result.attempts, cost, usage)
    except GatewayError as exc:
        rec.status_code, rec.error_type = exc.status, exc.etype
        if rec.attempts == 0 and exc.status != 503:  # rejected by policy before any provider call
            await st.metrics.inc("gateway_rejections_total", reason=exc.etype)
        raise
    finally:
        rec.latency_ms = int((time.monotonic() - started) * 1000)
        await st.metrics.inc("gateway_requests_total", requested_group=group, status=str(rec.status_code))
        await st.store.record(rec)
        if reserved:
            await st.limiter.release_budget(key.key_id, reserved)
        log.info(json.dumps({"request_id": rid, "team": key.team, "group": group, "status": rec.status_code,
                             "deployment": rec.deployment, "cache_hit": rec.cache_hit,
                             "fallback": rec.fallback_used, "attempts": rec.attempts,
                             "cost_usd": rec.cost_usd, "latency_ms": rec.latency_ms}))


def _ok(response: dict, rid: str, group: str, served: str, deployment: str, cache: str, fallback: bool,
        attempts: int, cost: float, usage: dict) -> JSONResponse:
    return JSONResponse(response, headers={
        "x-request-id": rid, "x-gateway-model-group": group, "x-gateway-served-group": served,
        "x-gateway-deployment": deployment, "x-gateway-cache": cache,
        "x-gateway-fallback": str(fallback).lower(), "x-gateway-attempts": str(attempts),
        "x-gateway-cost-usd": f"{cost:.8f}", "x-gateway-total-tokens": str(usage.get("total_tokens", 0))})


@app.post("/key/generate")
async def generate_key(request: Request, req: KeyRequest):
    require_master(request)
    st = request.app.state
    unknown = sorted(set(req.models) - st.settings.groups)
    if unknown:
        raise GatewayError(400, "invalid_request_error", f"unknown model groups: {unknown}")
    try:
        parse_duration(req.budget_duration)
    except ValueError as exc:
        raise GatewayError(400, "invalid_request_error", str(exc))
    raw = f"sk-{secrets.token_urlsafe(24)}"
    vk = await st.store.upsert_key(raw, **req.model_dump())
    return {"key": raw, "key_id": vk.key_id, "team": vk.team, "models": list(vk.models),
            "max_budget": vk.max_budget, "budget_duration": req.budget_duration, "rpm_limit": vk.rpm_limit,
            "tpm_limit": vk.tpm_limit, "note": "store this key now; only its SHA-256 hash is kept"}


@app.get("/v1/admin/costs")
async def costs(request: Request, team: str | None = None, days: int = Query(30, ge=1, le=365)):
    require_master(request)
    return await request.app.state.store.cost_report(team, days)


@app.get("/metrics", response_class=PlainTextResponse)
async def metrics(request: Request):
    require_master(request)
    return PlainTextResponse(await request.app.state.metrics.render(), media_type="text/plain; version=0.0.4")
