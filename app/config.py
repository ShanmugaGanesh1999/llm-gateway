"""Loads config/gateway.yaml (LiteLLM-like shape) into typed settings."""
import os
import re
from dataclasses import dataclass

import yaml

STRATEGIES = {"simple-shuffle", "least-busy"}


@dataclass(frozen=True)
class Deployment:
    id: str              # stable deployment id, e.g. "provider-a/mock-gpt-4o-mini"
    group: str           # logical model group, e.g. "smart"
    provider: str        # "openai" = OpenAI-compatible wire format
    model: str           # model id sent upstream
    api_base: str
    api_key: str
    timeout: float
    weight: int
    input_cost: float    # USD per prompt token
    output_cost: float   # USD per completion token


@dataclass(frozen=True)
class Settings:
    deployments: dict[str, list[Deployment]]
    routing_strategy: str
    num_retries: int
    retry_backoff: float
    allowed_fails: int
    cooldown_time: int
    fallbacks: dict[str, list[str]]
    cache_enabled: bool
    cache_ttl: int
    master_key: str
    policy_version: str
    auth_cache_ttl: int

    @property
    def groups(self) -> set[str]:
        return set(self.deployments)


def _env(value, env):
    """Resolve LiteLLM-style "os.environ/NAME" references."""
    if isinstance(value, str) and value.startswith("os.environ/"):
        name = value.split("/", 1)[1]
        if not env.get(name):
            raise ValueError(f"config references unset environment variable {name}")
        return env[name]
    return value


def parse_duration(text: str) -> int:
    """'30d' | '24h' | '15m' | '90s' -> seconds."""
    match = re.fullmatch(r"(\d+)([smhd])", str(text).strip())
    if not match:
        raise ValueError(f"invalid duration {text!r}; use e.g. 30d, 24h, 15m, 90s")
    return int(match[1]) * {"s": 1, "m": 60, "h": 3600, "d": 86400}[match[2]]


def cost_usd(dep: Deployment, prompt_tokens: int, completion_tokens: int) -> float:
    return round(prompt_tokens * dep.input_cost + completion_tokens * dep.output_cost, 10)


def parse_settings(raw: dict, env=os.environ) -> Settings:
    deployments: dict[str, list[Deployment]] = {}
    for entry in raw.get("model_list") or []:
        params, info = entry["litellm_params"], entry.get("model_info") or {}
        provider, _, model = params["model"].partition("/")
        dep = Deployment(
            id=info.get("id") or f"{entry['model_name']}/{params['model']}",
            group=entry["model_name"], provider=provider, model=model or provider,
            api_base=_env(params["api_base"], env), api_key=_env(params["api_key"], env),
            timeout=float(params.get("timeout", 30)), weight=int(params.get("weight", 1)),
            input_cost=float(info.get("input_cost_per_token", 0)),
            output_cost=float(info.get("output_cost_per_token", 0)))
        if provider != "openai":
            raise ValueError(f"{dep.id}: only OpenAI-compatible deployments are supported")
        deployments.setdefault(dep.group, []).append(dep)
    if not deployments:
        raise ValueError("model_list is empty")

    # Optional real upstream (CONVENTIONS section 4/6): replaces the `smart` group when both are set.
    base, key = env.get("REAL_UPSTREAM_BASE_URL"), env.get("REAL_UPSTREAM_API_KEY")
    if base and key and "smart" in deployments:
        model = env.get("REAL_UPSTREAM_MODEL", "gpt-4o-mini")
        deployments["smart"] = [Deployment(
            id=f"real/{model}", group="smart", provider="openai", model=model, api_base=base,
            api_key=key, timeout=30.0, weight=1,
            input_cost=float(env.get("REAL_UPSTREAM_INPUT_COST", "0.00000015")),
            output_cost=float(env.get("REAL_UPSTREAM_OUTPUT_COST", "0.0000006")))]

    router = raw.get("router_settings") or {}
    fallbacks: dict[str, list[str]] = {}
    for item in router.get("fallbacks") or []:
        for source, targets in item.items():
            fallbacks[source] = list(targets)
    for source, targets in fallbacks.items():
        unknown = [g for g in [source, *targets] if g not in deployments]
        if unknown or source in targets:
            raise ValueError(f"invalid fallback {source} -> {targets}: unknown or self-referencing group")
    strategy = router.get("routing_strategy", "simple-shuffle")
    if strategy not in STRATEGIES:
        raise ValueError(f"routing_strategy must be one of {sorted(STRATEGIES)}")

    cache = (raw.get("litellm_settings") or {})
    general = raw.get("general_settings") or {}
    return Settings(
        deployments=deployments, routing_strategy=strategy,
        num_retries=int(router.get("num_retries", 1)),
        retry_backoff=float(router.get("retry_backoff_seconds", 0.1)),
        allowed_fails=int(router.get("allowed_fails", 1)),
        cooldown_time=int(router.get("cooldown_time", 15)),
        fallbacks=fallbacks, cache_enabled=bool(cache.get("cache", False)),
        cache_ttl=int((cache.get("cache_params") or {}).get("ttl", 300)),
        master_key=_env(general.get("master_key", "os.environ/GATEWAY_MASTER_KEY"), env),
        policy_version=str(general.get("policy_version", "v1")),
        auth_cache_ttl=int(general.get("auth_cache_ttl", 30)))


def load_settings(path: str | None = None) -> Settings:
    with open(path or os.environ.get("GATEWAY_CONFIG", "config/gateway.yaml")) as fh:
        return parse_settings(yaml.safe_load(fh))
