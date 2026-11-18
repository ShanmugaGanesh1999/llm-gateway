CREATE TABLE IF NOT EXISTS virtual_keys (
  key_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  key_hash          CHAR(64) NOT NULL UNIQUE,          -- sha256(raw key); raw key never stored
  key_prefix        TEXT NOT NULL,                     -- e.g. "sk-demo-…am-a" for display
  key_alias         TEXT,
  team              TEXT NOT NULL,
  models            TEXT[] NOT NULL,                   -- allowlisted model groups
  max_budget_usd    NUMERIC(14, 6),                    -- NULL = unlimited
  budget_duration_s INTEGER NOT NULL DEFAULT 2592000,  -- rolling window, 30d
  rpm_limit         INTEGER,
  tpm_limit         INTEGER,
  revoked           BOOLEAN NOT NULL DEFAULT FALSE,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS spend_logs (
  id                BIGSERIAL PRIMARY KEY,
  request_id        TEXT NOT NULL,
  key_id            UUID NOT NULL REFERENCES virtual_keys (key_id),
  team              TEXT NOT NULL,
  requested_group   TEXT NOT NULL,                     -- what the app asked for ("smart")
  served_group      TEXT,                              -- group that answered ("backup" on fallback)
  deployment        TEXT,                              -- physical deployment id
  provider_model    TEXT,
  prompt_tokens     INTEGER NOT NULL DEFAULT 0,
  completion_tokens INTEGER NOT NULL DEFAULT 0,
  cost_usd          NUMERIC(14, 8) NOT NULL DEFAULT 0,
  cache_hit         BOOLEAN NOT NULL DEFAULT FALSE,
  fallback_used     BOOLEAN NOT NULL DEFAULT FALSE,
  attempts          INTEGER NOT NULL DEFAULT 0,        -- upstream calls made for this request
  latency_ms        INTEGER NOT NULL,
  status_code       INTEGER NOT NULL,
  error_type        TEXT,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS spend_logs_key_time ON spend_logs (key_id, created_at);
CREATE INDEX IF NOT EXISTS spend_logs_team_time ON spend_logs (team, created_at);
