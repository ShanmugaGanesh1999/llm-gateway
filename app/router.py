"""Deployment selection, bounded retries on transient errors, Redis cooldowns, one-hop fallback."""
import asyncio
import logging
import random
from dataclasses import dataclass

import httpx

from .config import Deployment, Settings

log = logging.getLogger("gateway.router")
TRANSIENT_STATUS = {408, 409, 429, 500, 502, 503, 504}
PASSTHROUGH_STATUS = {400, 404, 413, 422}  # caller's fault: return as-is, never fall back


def is_transient(status: int | None = None, exc: BaseException | None = None) -> bool:
    """Retry/fallback only when repeating the call can plausibly succeed."""
    if exc is not None:
        return isinstance(exc, httpx.TransportError)  # timeouts, connect errors, resets
    return status is not None and (status in TRANSIENT_STATUS or status >= 500)


class UpstreamError(Exception):
    def __init__(self, status: int, transient: bool, message: str):
        super().__init__(message)
        self.status, self.transient, self.message = status, transient, message


class RouteFailed(Exception):
    def __init__(self, status: int, etype: str, message: str, attempts: int,
                 deployment: Deployment | None = None, group: str | None = None):
        super().__init__(message)
        self.status, self.etype, self.message, self.attempts = status, etype, message, attempts
        self.deployment, self.group = deployment, group


@dataclass
class RouteResult:
    response: dict
    deployment: Deployment
    served_group: str
    attempts: int
    fallback_used: bool


class Router:
    def __init__(self, settings: Settings, redis, http: httpx.AsyncClient, metrics):
        self.s, self.redis, self.http, self.metrics = settings, redis, http, metrics

    async def healthy(self, group: str) -> list[Deployment]:
        deps = self.s.deployments[group]
        try:
            cooling = await self.redis.mget([f"cooldown:{d.id}" for d in deps])
        except Exception as exc:  # noqa: BLE001 - lose cooldowns rather than all traffic
            log.warning("cooldown read failed: %s", exc)
            cooling = [None] * len(deps)
        return [d for d, c in zip(deps, cooling) if not c]

    async def pick(self, deps: list[Deployment]) -> Deployment:
        if self.s.routing_strategy == "least-busy":
            counts = await self.redis.mget([f"inflight:{d.id}" for d in deps])
            load = [int(c or 0) for c in counts]
            return random.choice([d for d, n in zip(deps, load) if n == min(load)])
        return random.choices(deps, weights=[d.weight for d in deps])[0]

    async def call(self, dep: Deployment, payload: dict) -> dict:
        url = f"{dep.api_base.rstrip('/')}/chat/completions"
        await self.redis.incr(f"inflight:{dep.id}")
        try:
            resp = await self.http.post(url, json={**payload, "model": dep.model}, timeout=dep.timeout,
                                        headers={"Authorization": f"Bearer {dep.api_key}"})
        except httpx.TransportError as exc:
            status = 504 if isinstance(exc, httpx.TimeoutException) else 502
            raise UpstreamError(status, True, f"{dep.id}: {type(exc).__name__}") from exc
        finally:
            await self.redis.decr(f"inflight:{dep.id}")
        if resp.status_code != 200:
            try:
                detail = resp.json()["error"]["message"]
            except Exception:  # noqa: BLE001
                detail = resp.text[:200]
            raise UpstreamError(resp.status_code, is_transient(resp.status_code), f"{dep.id}: {detail}")
        data = resp.json()
        if not data.get("choices"):
            raise UpstreamError(502, True, f"{dep.id}: malformed response without choices")
        return data

    async def mark_failure(self, dep: Deployment) -> None:
        """More than `allowed_fails` failures within 60 s puts the deployment in cooldown."""
        key = f"fails:{dep.id}"
        fails = await self.redis.incr(key)
        if fails == 1:
            await self.redis.expire(key, 60)
        if fails > self.s.allowed_fails:
            await self.redis.set(f"cooldown:{dep.id}", "1", ex=self.s.cooldown_time)
            await self.redis.delete(key)
            await self.metrics.inc("gateway_upstream_attempts_total", deployment=dep.id, outcome="cooldown")
            log.warning("deployment %s cooling down for %ss", dep.id, self.s.cooldown_time)

    async def route(self, group: str, payload: dict) -> RouteResult:
        groups = [group, *self.s.fallbacks.get(group, [])]  # one hop: fallbacks are not transitive
        attempts, last = 0, None
        for hop, current in enumerate(groups):
            for retry in range(1 + self.s.num_retries):
                deps = await self.healthy(current)
                if not deps:
                    break
                dep = await self.pick(deps)
                if retry:
                    await asyncio.sleep(self.s.retry_backoff * retry * random.uniform(0.5, 1.5))
                attempts += 1
                try:
                    data = await self.call(dep, payload)
                except UpstreamError as err:
                    await self.metrics.inc("gateway_upstream_attempts_total", deployment=dep.id,
                                           outcome=str(err.status))
                    if not err.transient:
                        status = err.status if err.status in PASSTHROUGH_STATUS else 502
                        raise RouteFailed(status, "upstream_rejected", err.message, attempts, dep, current) from err
                    last = err
                    await self.mark_failure(dep)
                    continue
                await self.metrics.inc("gateway_upstream_attempts_total", deployment=dep.id, outcome="ok")
                return RouteResult(data, dep, current, attempts, fallback_used=hop > 0)
        reason = last.message if last else "all deployments are cooling down"
        raise RouteFailed(503, "no_healthy_deployment",
                          f"groups {groups} unavailable after {attempts} attempt(s); last error: {reason}", attempts)
