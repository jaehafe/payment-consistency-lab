-- 06-balance-concurrency 스키마 (docs/SPEC.md Part 2 와 동일)
CREATE TABLE IF NOT EXISTS accounts (
  id       TEXT PRIMARY KEY,
  balance  BIGINT NOT NULL,          -- 의도적으로 CHECK(balance >= 0) 없음: 나이브 구현의 초과 인출을 보이기 위해
  version  BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS withdrawals (
  id          BIGSERIAL PRIMARY KEY,
  account_id  TEXT NOT NULL,
  amount      BIGINT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
