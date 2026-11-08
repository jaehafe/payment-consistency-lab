-- 이 단계의 테이블은 스키마 k10에 만든다. 커넥션의 search_path 는 application.yml 의
-- spring.datasource.hikari.schema=k10 가 맞춰 두므로 아래 DDL에는 스키마 접두어가 없다.
CREATE SCHEMA IF NOT EXISTS k10;

-- 07의 payments를 간소화 (history/janitor 컬럼 생략). 관측성이 주제다.
CREATE TABLE IF NOT EXISTS payments (
  id              UUID PRIMARY KEY,
  order_id        TEXT NOT NULL,
  amount          BIGINT NOT NULL,
  status          TEXT NOT NULL,          -- REQUESTED | PENDING | APPROVED | FAILED | UNKNOWN
  idempotency_key TEXT NOT NULL UNIQUE,   -- 클라이언트 멱등키 (02의 간소화 버전)
  pg_tx_id        TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS payments_unknown_idx ON payments (status) WHERE status = 'UNKNOWN';
