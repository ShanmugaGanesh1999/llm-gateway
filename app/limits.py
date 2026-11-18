"""Redis-backed, multi-worker-safe per-key RPM/TPM limits and in-flight budget reservations.

Rate limits use a sliding-window *counter* (two fixed 60 s buckets, the previous one weighted by
how much of it still overlaps the window). The check-and-increment runs in one Lua script, so
concurrent requests on any worker or replica cannot both take the last slot.
"""
import math
import time
from dataclasses import dataclass

WINDOW_S = 60

RATE_LUA = """
-- KEYS: rpm_cur, rpm_prev, tpm_cur, tpm_prev
-- ARGV: rpm_limit, tpm_limit, tokens, prev_weight, ttl   (limit 0 = unlimited)
local w = tonumber(ARGV[4])
local function used(cur, prev)
  return tonumber(redis.call('GET', cur) or '0') + math.ceil(tonumber(redis.call('GET', prev) or '0') * w)
end
local rpm, tpm, tok = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
if rpm > 0 and used(KEYS[1], KEYS[2]) + 1 > rpm then return 'rpm' end
if tpm > 0 and used(KEYS[3], KEYS[4]) + tok > tpm then return 'tpm' end
redis.call('INCR', KEYS[1]); redis.call('EXPIRE', KEYS[1], ARGV[5])
redis.call('INCRBY', KEYS[3], tok); redis.call('EXPIRE', KEYS[3], ARGV[5])
return 'ok'
"""

RESERVE_LUA = """
-- KEYS: reserved ; ARGV: spent, budget, amount
local reserved = tonumber(redis.call('GET', KEYS[1]) or '0')
if tonumber(ARGV[1]) + reserved >= tonumber(ARGV[2]) then return 0 end
redis.call('INCRBYFLOAT', KEYS[1], ARGV[3]); redis.call('EXPIRE', KEYS[1], 300)
return 1
"""

RELEASE_LUA = """
local v = tonumber(redis.call('INCRBYFLOAT', KEYS[1], -tonumber(ARGV[1])))
if v <= 1e-12 then redis.call('DEL', KEYS[1]) end
return 1
"""


@dataclass(frozen=True)
class RateDecision:
    allowed: bool
    reason: str          # ok | rpm | tpm
    bucket: int          # bucket the tokens were reserved in (for later adjustment)
    retry_after: int


def window_position(now: float) -> tuple[int, float]:
    """Current bucket id and the weight of the previous bucket still inside the sliding window."""
    bucket = int(now // WINDOW_S)
    return bucket, 1.0 - (now % WINDOW_S) / WINDOW_S


def rl_key(key_id: str, kind: str, bucket: int) -> str:
    return f"rl:{{{key_id}}}:{kind}:{bucket}"  # {hash tag} keeps all keys of one script on one cluster slot


class Limiter:
    def __init__(self, redis):
        self.redis = redis
        self._rate = redis.register_script(RATE_LUA)
        self._reserve = redis.register_script(RESERVE_LUA)
        self._release = redis.register_script(RELEASE_LUA)

    async def check_rate(self, key_id: str, rpm: int | None, tpm: int | None, tokens: int,
                         now: float | None = None) -> RateDecision:
        now = time.time() if now is None else now
        bucket, weight = window_position(now)
        keys = [rl_key(key_id, "rpm", bucket), rl_key(key_id, "rpm", bucket - 1),
                rl_key(key_id, "tpm", bucket), rl_key(key_id, "tpm", bucket - 1)]
        reason = await self._rate(keys=keys, args=[rpm or 0, tpm or 0, tokens, weight, WINDOW_S * 2])
        retry_after = max(1, math.ceil(WINDOW_S - now % WINDOW_S))
        return RateDecision(reason == "ok", reason, bucket, retry_after)

    async def adjust_tokens(self, key_id: str, bucket: int, delta: int) -> None:
        """Replace the up-front token estimate with actual usage once the response is known."""
        if delta:
            await self.redis.incrby(rl_key(key_id, "tpm", bucket), delta)

    async def reserve_budget(self, key_id: str, spent: float, budget: float, amount: float) -> bool:
        """Admit only if durable spend plus other in-flight reservations is still under budget."""
        return bool(await self._reserve(keys=[f"budget:{{{key_id}}}:reserved"], args=[spent, budget, amount]))

    async def release_budget(self, key_id: str, amount: float) -> None:
        await self._release(keys=[f"budget:{{{key_id}}}:reserved"], args=[amount])
