"""Prometheus counters stored in one Redis hash so every worker/replica reports the same totals."""
import logging

log = logging.getLogger("gateway.metrics")
HASH = "metrics"
HELP = {
    "gateway_requests_total": "Chat completion requests by requested group and HTTP status",
    "gateway_cache_total": "Response cache lookups by result",
    "gateway_upstream_attempts_total": "Upstream provider calls by deployment and outcome",
    "gateway_fallbacks_total": "Requests served by a fallback group",
    "gateway_rejections_total": "Requests rejected by policy before any provider call",
    "gateway_cost_usd_total": "Provider cost attributed to teams (USD)",
    "gateway_tokens_total": "Provider tokens by team and kind",
}


def series(name: str, **labels) -> str:
    inner = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
    return f"{name}{{{inner}}}" if inner else name


class Metrics:
    def __init__(self, redis):
        self.redis = redis

    async def inc(self, name: str, amount: float = 1, **labels) -> None:
        try:
            if isinstance(amount, float):
                await self.redis.hincrbyfloat(HASH, series(name, **labels), amount)
            else:
                await self.redis.hincrby(HASH, series(name, **labels), amount)
        except Exception as exc:  # noqa: BLE001 - metrics are best effort
            log.warning("metrics write failed: %s", exc)

    async def render(self) -> str:
        values = await self.redis.hgetall(HASH)
        lines = []
        for name, help_text in HELP.items():
            lines += [f"# HELP {name} {help_text}", f"# TYPE {name} counter"]
            lines += [f"{k} {v}" for k, v in sorted(values.items()) if k.split("{")[0] == name]
        return "\n".join(lines) + "\n"
