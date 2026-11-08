-- 복식부기 원장. 금액은 전부 원 단위 정수(BIGINT) — 부동소수점은 0.1 + 0.2 ≠ 0.3 이라 돈에 쓰면 안 된다.
CREATE TABLE IF NOT EXISTS ledger_accounts (
  id       TEXT PRIMARY KEY,         -- 'pg:receivable', 'bank:cash', 'platform:fee_revenue', 'merchant:{id}:payable'
  type     TEXT NOT NULL,            -- ASSET | LIABILITY | REVENUE
  balance  BIGINT NOT NULL DEFAULT 0 -- 차변 +, 대변 - (부호 규칙: debit-positive)
);
CREATE TABLE IF NOT EXISTS journal_entries (
  id              UUID PRIMARY KEY,
  reference_type  TEXT NOT NULL,     -- PAYMENT | REFUND | PG_DEPOSIT | PAYOUT
  reference_id    TEXT NOT NULL,
  description     TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (reference_type, reference_id)   -- 같은 거래 두 번 전기 방지 (원장 멱등성)
);
CREATE TABLE IF NOT EXISTS ledger_lines (
  id          BIGSERIAL PRIMARY KEY,
  entry_id    UUID NOT NULL REFERENCES journal_entries(id),
  account_id  TEXT NOT NULL REFERENCES ledger_accounts(id),
  debit       BIGINT NOT NULL DEFAULT 0,
  credit      BIGINT NOT NULL DEFAULT 0,
  CHECK (debit >= 0 AND credit >= 0 AND (debit = 0) <> (credit = 0))  -- 한 줄은 차변 또는 대변 한쪽만
);
CREATE TABLE IF NOT EXISTS transactions (            -- 결제/환불 원천 거래
  id             UUID PRIMARY KEY,
  merchant_id    TEXT NOT NULL,
  type           TEXT NOT NULL,        -- PAYMENT | REFUND
  pg_tx_id       TEXT NOT NULL,
  amount         BIGINT NOT NULL,      -- 항상 양수
  fee            BIGINT NOT NULL,      -- 환불은 수수료 환급(음수 아님, 별도 표시)
  occurred_at    TIMESTAMPTZ NOT NULL,
  settlement_id  UUID
);
CREATE TABLE IF NOT EXISTS settlements (
  id               UUID PRIMARY KEY,
  merchant_id      TEXT NOT NULL,
  settlement_date  DATE NOT NULL,      -- KST 기준 거래일
  gross_amount     BIGINT NOT NULL,
  refund_amount    BIGINT NOT NULL,
  fee_amount       BIGINT NOT NULL,    -- 순수수료 (결제 수수료 - 환불 수수료 환급)
  net_amount       BIGINT NOT NULL,    -- gross - refund - fee
  status           TEXT NOT NULL,      -- CALCULATED | PAID
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (merchant_id, settlement_date)
);
