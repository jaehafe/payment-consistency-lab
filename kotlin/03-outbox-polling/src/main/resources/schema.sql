CREATE TABLE IF NOT EXISTS payments (
  id          UUID PRIMARY KEY,
  order_id    TEXT NOT NULL UNIQUE,
  amount      BIGINT NOT NULL,
  status      TEXT NOT NULL,          -- COMPLETED
  pg_tx_id    TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 테이블 이름이 SPEC의 `outbox`가 아니라 `outbox_polling`인 이유:
-- 로컬에서는 03과 04가 같은 Postgres(lab DB, public 스키마)를 쓴다. 04의 Debezium 커넥터는
-- `public.outbox`(Debezium 기본 컬럼명)를 보므로, 03이 같은 이름에 다른 컬럼으로 먼저 만들면
-- 04의 `CREATE TABLE IF NOT EXISTS outbox`가 그냥 넘어가고 04의 insert가 깨진다.
-- (Go 03은 같은 문제를 별도 스키마 go_03 으로 피한다.) 컬럼은 SPEC과 같다.
CREATE TABLE IF NOT EXISTS outbox_polling (
  id              UUID PRIMARY KEY,         -- = message_id 헤더
  aggregate_type  TEXT NOT NULL,            -- 'payment'
  aggregate_id    TEXT NOT NULL,            -- orderId (= Kafka key)
  event_type      TEXT NOT NULL,            -- 'PaymentCompleted'
  payload         JSONB NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at    TIMESTAMPTZ
);
-- 아직 발행 안 된 행만 담는 부분 인덱스: 발행 완료된 행이 아무리 쌓여도 relay의 조회 비용은 작게 유지된다.
CREATE INDEX IF NOT EXISTS outbox_polling_unpublished_idx ON outbox_polling (created_at) WHERE published_at IS NULL;
