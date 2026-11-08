-- 07-payment-state-machine 스키마 (docs/SPEC.md Part 2 와 동일)
CREATE TABLE IF NOT EXISTS payments (
  id              UUID PRIMARY KEY,
  order_id        TEXT NOT NULL UNIQUE,
  amount          BIGINT NOT NULL,
  status          TEXT NOT NULL,
  idempotency_key TEXT NOT NULL UNIQUE,   -- PG 호출용
  pg_tx_id        TEXT,
  attempt_count   INT NOT NULL DEFAULT 0, -- janitor 확인 시도 횟수
  next_retry_at   TIMESTAMPTZ,
  last_error      TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS payment_status_history (
  id          BIGSERIAL PRIMARY KEY,
  payment_id  UUID NOT NULL,
  from_status TEXT,
  to_status   TEXT NOT NULL,
  reason      TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- janitor 가 "UNKNOWN 이고 재시도 시각이 된 것"만 빠르게 찾도록 하는 부분 인덱스
CREATE INDEX IF NOT EXISTS payments_retry_idx ON payments (next_retry_at)
  WHERE status IN ('UNKNOWN', 'CANCEL_REQUESTED');

-- 오래된 PENDING 조회용 (PENDING 엔 next_retry_at 이 없어 created_at 으로 고른다)
CREATE INDEX IF NOT EXISTS payments_pending_idx ON payments (created_at)
  WHERE status = 'PENDING';
