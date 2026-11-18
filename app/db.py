"""PostgreSQL: durable virtual keys (hashed) and spend logs."""
import asyncio
import hashlib
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path

import asyncpg
import yaml

from .config import parse_duration

log = logging.getLogger("gateway.db")
ROOT = Path(__file__).resolve().parent.parent
SCHEMA_SQL = ROOT / "data" / "schema.sql"
SEED_KEYS = ROOT / "data" / "seed_keys.yaml"


def hash_key(raw: str) -> str:
    return hashlib.sha256(raw.encode()).hexdigest()


def key_prefix(raw: str) -> str:
    return f"{raw[:7]}...{raw[-4:]}"


@dataclass(frozen=True)
class VirtualKey:
    key_id: str
    team: str
    alias: str | None
    models: tuple[str, ...]
    max_budget: float | None
    budget_duration_s: int
    rpm_limit: int | None
    tpm_limit: int | None


@dataclass
class SpendRecord:
    request_id: str
    key: VirtualKey
    requested_group: str = "-"
    served_group: str | None = None
    deployment: str | None = None
    provider_model: str | None = None
    prompt_tokens: int = 0
    completion_tokens: int = 0
    cost_usd: float = 0.0
    cache_hit: bool = False
    fallback_used: bool = False
    attempts: int = 0
    latency_ms: int = 0
    status_code: int = 500
    error_type: str | None = None
    extra: dict = field(default_factory=dict)


def _row_to_key(row) -> VirtualKey:
    return VirtualKey(
        key_id=str(row["key_id"]), team=row["team"], alias=row["key_alias"], models=tuple(row["models"]),
        max_budget=None if row["max_budget_usd"] is None else float(row["max_budget_usd"]),
        budget_duration_s=row["budget_duration_s"], rpm_limit=row["rpm_limit"], tpm_limit=row["tpm_limit"])


class Store:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    @classmethod
    async def connect(cls, dsn: str, attempts: int = 30) -> "Store":
        for i in range(attempts):
            try:
                return cls(await asyncpg.create_pool(dsn, min_size=1, max_size=10))
            except (OSError, asyncpg.PostgresError) as exc:
                log.warning("postgres not ready (%s), retry %d", exc, i + 1)
                await asyncio.sleep(1)
        raise RuntimeError("could not connect to Postgres")

    async def init(self) -> None:
        """Create schema and upsert demo keys. Advisory lock: several workers start at once."""
        seeds = yaml.safe_load(SEED_KEYS.read_text())
        async with self.pool.acquire() as con, con.transaction():
            await con.execute("SELECT pg_advisory_xact_lock(4202)")
            await con.execute(SCHEMA_SQL.read_text())
            for seed in seeds:
                await self.upsert_key(seed.pop("key"), **seed, con=con)

    async def upsert_key(self, raw: str, team: str, models: list[str], key_alias: str | None = None,
                         max_budget: float | None = None, budget_duration: str = "30d",
                         rpm_limit: int | None = None, tpm_limit: int | None = None, con=None) -> VirtualKey:
        sql = """
        INSERT INTO virtual_keys (key_hash, key_prefix, key_alias, team, models, max_budget_usd,
                                  budget_duration_s, rpm_limit, tpm_limit)
        VALUES ($1, $2, $3, $4, $5, $6::float8, $7, $8, $9)
        ON CONFLICT (key_hash) DO UPDATE SET key_alias = EXCLUDED.key_alias, team = EXCLUDED.team,
          models = EXCLUDED.models, max_budget_usd = EXCLUDED.max_budget_usd,
          budget_duration_s = EXCLUDED.budget_duration_s, rpm_limit = EXCLUDED.rpm_limit,
          tpm_limit = EXCLUDED.tpm_limit, revoked = FALSE
        RETURNING *"""
        args = (hash_key(raw), key_prefix(raw), key_alias, team, list(models), max_budget,
                parse_duration(budget_duration), rpm_limit, tpm_limit)
        row = await (con or self.pool).fetchrow(sql, *args)
        return _row_to_key(row)

    async def get_key(self, key_hash: str) -> VirtualKey | None:
        row = await self.pool.fetchrow(
            "SELECT * FROM virtual_keys WHERE key_hash = $1 AND NOT revoked", key_hash)
        return _row_to_key(row) if row else None

    async def spent(self, key: VirtualKey) -> float:
        value = await self.pool.fetchval(
            """SELECT COALESCE(SUM(cost_usd), 0) FROM spend_logs
               WHERE key_id = $1::uuid AND created_at > now() - make_interval(secs => $2)""",
            key.key_id, key.budget_duration_s)
        return float(value)

    async def record(self, r: SpendRecord) -> None:
        await self.pool.execute(
            """INSERT INTO spend_logs (request_id, key_id, team, requested_group, served_group, deployment,
                 provider_model, prompt_tokens, completion_tokens, cost_usd, cache_hit, fallback_used,
                 attempts, latency_ms, status_code, error_type)
               VALUES ($1,$2::uuid,$3,$4,$5,$6,$7,$8,$9,$10::float8,$11,$12,$13,$14,$15,$16)""",
            r.request_id, r.key.key_id, r.key.team, r.requested_group, r.served_group, r.deployment,
            r.provider_model, r.prompt_tokens, r.completion_tokens, r.cost_usd, r.cache_hit,
            r.fallback_used, r.attempts, r.latency_ms, r.status_code, r.error_type)

    async def cost_report(self, team: str | None, days: int) -> dict:
        rows = await self.pool.fetch(
            """SELECT team, requested_group, COALESCE(served_group, '-') AS served_group,
                      COALESCE(deployment, CASE WHEN cache_hit THEN 'cache' ELSE '-' END) AS deployment,
                      status_code, cache_hit, fallback_used,
                      count(*) AS requests, SUM(cost_usd) AS cost, SUM(prompt_tokens) AS pt,
                      SUM(completion_tokens) AS ct, SUM(attempts) AS attempts
               FROM spend_logs
               WHERE ($1::text IS NULL OR team = $1) AND created_at > now() - make_interval(days => $2)
               GROUP BY 1, 2, 3, 4, 5, 6, 7""", team, days)
        return summarize(rows, team, days)


def _bucket() -> dict:
    return {"requests": 0, "successful": 0, "cost_usd": 0.0, "prompt_tokens": 0,
            "completion_tokens": 0, "cache_hits": 0, "fallbacks": 0, "upstream_attempts": 0}


def summarize(rows, team: str | None, days: int) -> dict:
    """Aggregate grouped spend rows into totals and by-team/group/deployment views."""
    total, views = _bucket(), {"by_team": {}, "by_requested_group": {}, "by_deployment": {}}
    errors: dict[str, int] = {}
    for row in rows:
        keys = {"by_team": row["team"], "by_requested_group": row["requested_group"],
                "by_deployment": row["deployment"]}
        targets = [total] + [views[v].setdefault(k, _bucket()) for v, k in keys.items()]
        ok = row["status_code"] == 200
        if not ok:
            errors[str(row["status_code"])] = errors.get(str(row["status_code"]), 0) + row["requests"]
        for b in targets:
            b["requests"] += row["requests"]
            b["successful"] += row["requests"] if ok else 0
            b["cost_usd"] += float(row["cost"] or 0)
            b["prompt_tokens"] += int(row["pt"] or 0)
            b["completion_tokens"] += int(row["ct"] or 0)
            b["cache_hits"] += row["requests"] if (ok and row["cache_hit"]) else 0
            b["fallbacks"] += row["requests"] if (ok and row["fallback_used"]) else 0
            b["upstream_attempts"] += int(row["attempts"] or 0)
    for b in [total, *(b for view in views.values() for b in view.values())]:
        _rates(b)
    return {"team": team, "window_days": days, "total": total, **views, "errors_by_status": errors}


def _rates(b: dict) -> None:
    ok, served = b["successful"], b["successful"] - b["cache_hits"]
    b["cost_usd"] = round(b["cost_usd"], 8)
    b["cache_hit_rate"] = round(b["cache_hits"] / ok, 4) if ok else 0.0
    b["fallback_rate"] = round(b["fallbacks"] / served, 4) if served else 0.0
    b["cost_per_successful_request_usd"] = round(b["cost_usd"] / ok, 8) if ok else 0.0


DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://gateway:gateway@localhost:5432/gateway")
