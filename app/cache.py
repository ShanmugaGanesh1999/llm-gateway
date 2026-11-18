"""Tenant-scoped exact-match response cache in Redis."""
import hashlib
import json
import logging

log = logging.getLogger("gateway.cache")
NOT_IN_KEY = {"messages", "user", "cache", "model", "stream"}  # everything else changes the output


def normalize_messages(messages: list[dict]) -> list[dict]:
    """Strip surrounding whitespace and CRLF noise; keep every other field (name, tool_call_id, ...)."""
    out = []
    for m in messages:
        item = {k: v for k, v in sorted(m.items()) if v is not None}
        if isinstance(item.get("content"), str):
            item["content"] = item["content"].replace("\r\n", "\n").strip()
        out.append(item)
    return out


def cache_key(team: str, group: str, messages: list[dict], params: dict, policy_version: str) -> str:
    """sha256 over everything that can change the answer or who may see it."""
    material = {
        "policy": policy_version, "team": team, "group": group,
        "messages": normalize_messages(messages),
        "params": {k: v for k, v in sorted(params.items()) if k not in NOT_IN_KEY},
    }
    digest = hashlib.sha256(json.dumps(material, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return f"cache:{team}:{group}:{digest}"  # team prefix allows per-tenant invalidation by SCAN


class ResponseCache:
    """Fail-open: a Redis problem degrades to a cache miss, never to a failed request."""

    def __init__(self, redis, ttl: int, enabled: bool):
        self.redis, self.ttl, self.enabled = redis, ttl, enabled

    async def get(self, key: str) -> dict | None:
        if not self.enabled:
            return None
        try:
            raw = await self.redis.get(key)
            return json.loads(raw) if raw else None
        except Exception as exc:  # noqa: BLE001 - cache must never break the request path
            log.warning("cache read failed: %s", exc)
            return None

    async def set(self, key: str, entry: dict) -> None:
        if not self.enabled:
            return
        try:
            await self.redis.set(key, json.dumps(entry), ex=self.ttl)
        except Exception as exc:  # noqa: BLE001
            log.warning("cache write failed: %s", exc)
