-- 10-observability 스키마: 07 의 결제 흐름을 간소화한 버전 (상태 머신/janitor 는 07 참고)
CREATE TABLE IF NOT EXISTS payments (
  id              UUID PRIMARY KEY,
  order_id        TEXT NOT NULL,
  amount          BIGINT NOT NULL,
  status          TEXT NOT NULL,           -- PENDING | APPROVED | FAILED | UNKNOWN
  idempotency_key TEXT NOT NULL UNIQUE,    -- 같은 key 재요청 → 저장된 결과를 replay
  pg_tx_id        TEXT,
  last_error      TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- payment_unknown_current 게이지가 스크레이프마다 count 하므로 부분 인덱스로 가볍게
CREATE INDEX IF NOT EXISTS payments_unknown_idx ON payments (created_at) WHERE status = 'UNKNOWN';
