CREATE TABLE IF NOT EXISTS payments (
  id              UUID PRIMARY KEY,
  order_id        TEXT NOT NULL UNIQUE,
  amount          BIGINT NOT NULL,
  status          TEXT NOT NULL,
  idempotency_key TEXT NOT NULL UNIQUE,   -- PG 호출용 (재조회/재시도 시 같은 key 사용)
  pg_tx_id        TEXT,
  attempt_count   INT NOT NULL DEFAULT 0, -- janitor 확인 시도 횟수
  next_retry_at   TIMESTAMPTZ,            -- janitor가 다음에 확인할 시각 (지수 백오프)
  last_error      TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- janitor 조회용: 처리 대기 중인 상태만 담는 부분 인덱스
CREATE INDEX IF NOT EXISTS payments_janitor_idx ON payments (next_retry_at)
  WHERE status IN ('UNKNOWN', 'CANCEL_REQUESTED');

-- 모든 상태 전이의 감사 로그. "왜 이 결제가 이 상태인가?"에 답할 수 있어야 한다.
CREATE TABLE IF NOT EXISTS payment_status_history (
  id          BIGSERIAL PRIMARY KEY,
  payment_id  UUID NOT NULL,
  from_status TEXT,
  to_status   TEXT NOT NULL,
  reason      TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
