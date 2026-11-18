"""Deterministic OpenAI-compatible mock provider with fault injection (CONVENTIONS section 6).

One image runs twice (gw-provider-a, gw-provider-b), told apart by PROVIDER_NAME. Single worker,
so the in-memory fault state below is consistent.
"""
import asyncio
import os
import re
import time
import uuid
from typing import Literal

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

NAME = os.environ.get("PROVIDER_NAME", "provider")
TIMEOUT_SLEEP_S = float(os.environ.get("TIMEOUT_SLEEP_S", "10"))
MAX_TOKENS_LIMIT = 4096

app = FastAPI(title=f"Mock LLM provider ({NAME})")
STATE: dict = {"mode": "ok", "remaining": None, "requests": 0, "faults_served": 0}
EVIDENCE = re.compile(r"^\s*\[C(\d+)\]\s*(.+)$", re.MULTILINE)
QUESTION = re.compile(r"^\s*Question:\s*(.+)$", re.MULTILINE)
WORD = re.compile(r"[a-z0-9-]+")
STOP = {"a", "an", "the", "is", "are", "do", "does", "what", "how", "many", "much", "i", "can",
        "of", "to", "for", "in", "on", "and", "or", "my", "we", "our", "be", "need", "needs"}


class Fault(BaseModel):
    mode: Literal["ok", "429", "500", "timeout"]
    count: int | None = Field(default=None, ge=1)  # None = until reset


def _error(status: int, message: str, etype: str, headers: dict | None = None) -> JSONResponse:
    body = {"error": {"message": message, "type": etype, "code": str(status)}}
    return JSONResponse(body, status_code=status, headers=headers)


def _text(content) -> str:
    if isinstance(content, list):  # OpenAI content parts
        return " ".join(p.get("text", "") for p in content if isinstance(p, dict))
    return str(content or "")


def _terms(text: str) -> set[str]:
    """Lower-cased words with a trailing plural 's' removed (means -> mean, codes -> code)."""
    return {w[:-1] if len(w) > 3 and w.endswith("s") and not w.endswith("ss") else w
            for w in WORD.findall(text.lower())}


def reply_for(messages: list[dict]) -> str:
    users = [m for m in messages if m.get("role") == "user"] or messages
    text = _text(users[-1].get("content")) if users else ""
    evidence = EVIDENCE.findall(text)
    if evidence:
        # Deterministic "generation": cite the one or two evidence sentences sharing the most
        # content words with the question (identifiers such as DEPLOY-4021 count triple).
        # Heading fragments under 4 words are skipped. With no question line this returns the
        # first full sentence of [C1].
        q = QUESTION.search(text)
        q_words = _terms(q.group(1)) - STOP if q else set()
        scored = []
        for pos, (ref, line) in enumerate(evidence):
            for spos, sentence in enumerate(re.split(r"(?<=[.!?])\s+", line.strip())):
                if len(WORD.findall(sentence.lower())) < 4:
                    continue
                score = sum(3 if any(c.isdigit() for c in w) else 1
                            for w in q_words & _terms(sentence))
                scored.append((score, -pos, -spos, sentence, ref))
        if not scored:  # only fragments: return the first evidence line as-is
            return f"{evidence[0][1].strip()} [C{evidence[0][0]}]"
        scored.sort(reverse=True)
        top = scored[0]
        picked = [top] + [s for s in scored[1:2] if top[0] > 0 and s[0] >= 0.75 * top[0]]
        return " ".join(f"{s[3]} [C{s[4]}]" for s in picked)
    return f"[{NAME}] {text[:160]}"


def _take_fault() -> str:
    mode = STATE["mode"]
    if mode != "ok":
        STATE["faults_served"] += 1
        if STATE["remaining"] is not None:
            STATE["remaining"] -= 1
            if STATE["remaining"] <= 0:
                STATE["mode"], STATE["remaining"] = "ok", None
    return mode


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


@app.post("/admin/fault")
async def set_fault(fault: Fault):
    STATE["mode"], STATE["remaining"] = fault.mode, (None if fault.mode == "ok" else fault.count)
    return {"provider": NAME, "mode": STATE["mode"], "remaining": STATE["remaining"]}


@app.get("/admin/stats")
async def stats():
    return {"provider": NAME, **STATE}


@app.post("/admin/reset")
async def reset():
    STATE.update(mode="ok", remaining=None, requests=0, faults_served=0)
    return {"provider": NAME, **STATE}


@app.post("/v1/chat/completions")
async def chat(request: Request):
    STATE["requests"] += 1
    if not request.headers.get("authorization", "").startswith("Bearer "):
        return _error(401, "missing provider API key", "authentication_error")
    mode = _take_fault()
    if mode == "429":
        return _error(429, f"{NAME}: rate limit reached", "rate_limit_error", {"Retry-After": "1"})
    if mode == "500":
        return _error(500, f"{NAME}: internal server error", "server_error")
    if mode == "timeout":
        await asyncio.sleep(TIMEOUT_SLEEP_S)

    body = await request.json()
    messages = body.get("messages")
    if not isinstance(messages, list) or not messages:
        return _error(400, "messages must be a non-empty list", "invalid_request_error")
    max_tokens = body.get("max_tokens")
    if max_tokens is not None and (not isinstance(max_tokens, int) or not 1 <= max_tokens <= MAX_TOKENS_LIMIT):
        return _error(400, f"max_tokens must be between 1 and {MAX_TOKENS_LIMIT}", "invalid_request_error")

    reply = reply_for(messages)
    if max_tokens:
        reply = reply[: max_tokens * 4]
    prompt_tokens = max(1, sum(len(_text(m.get("content"))) for m in messages) // 4)
    completion_tokens = max(1, len(reply) // 4)
    return {
        "id": f"chatcmpl-{uuid.uuid4().hex[:24]}",
        "object": "chat.completion",
        "created": int(time.time()),
        "model": body.get("model", "mock"),
        "system_fingerprint": NAME,
        "choices": [{"index": 0, "message": {"role": "assistant", "content": reply}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": prompt_tokens, "completion_tokens": completion_tokens,
                  "total_tokens": prompt_tokens + completion_tokens},
    }
