-- 05-idempotent-consumer 스키마 (docs/SPEC.md 와 동일). 결제 이벤트를 "받는 쪽"(주문/포인트 서비스) DB.
CREATE TABLE IF NOT EXISTS orders (
  id          TEXT PRIMARY KEY,
  status      TEXT NOT NULL,      -- PENDING | PAID
  paid_amount BIGINT NOT NULL DEFAULT 0
);

-- 결제 금액의 1% 적립. 중복 처리되면 두 번 쌓인다 (자연적으로 멱등하지 않은 부작용)
CREATE TABLE IF NOT EXISTS point_ledger (
  id          BIGSERIAL PRIMARY KEY,
  order_id    TEXT NOT NULL,
  points      BIGINT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS processed_messages (
  consumer_group TEXT NOT NULL,
  message_id     TEXT NOT NULL,
  processed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer_group, message_id)
);
