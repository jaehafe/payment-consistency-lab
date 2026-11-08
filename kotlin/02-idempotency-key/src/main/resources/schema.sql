-- 이 단계의 테이블은 스키마 k02에 만든다. 커넥션의 search_path 는 application.yml 의
-- spring.datasource.hikari.schema=k02 가 맞춰 두므로 아래 DDL에는 스키마 접두어가 없다.
CREATE SCHEMA IF NOT EXISTS k02;

CREATE TABLE IF NOT EXISTS payments (
  id          UUID PRIMARY KEY,
  order_id    TEXT NOT NULL UNIQUE,
  amount      BIGINT NOT NULL,
  status      TEXT NOT NULL,          -- COMPLETED
  pg_tx_id    TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Brandur, "Implementing Stripe-like Idempotency Keys in Postgres" (rocket-rides-atomic) 방식
CREATE TABLE IF NOT EXISTS idempotency_keys (
  id              BIGSERIAL PRIMARY KEY,
  idempotency_key TEXT NOT NULL UNIQUE,
  request_path    TEXT NOT NULL,
  request_hash    TEXT NOT NULL,       -- 요청 바디 SHA-256
  locked_at       TIMESTAMPTZ,         -- 처리 중 표시 (NULL = 잠금 없음)
  recovery_point  TEXT NOT NULL,       -- started | pg_charged | finished
  response_code   INT,
  response_body   JSONB,
  pg_tx_id        TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_run_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
