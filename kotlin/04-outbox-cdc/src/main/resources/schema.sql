CREATE TABLE IF NOT EXISTS payments (
  id          UUID PRIMARY KEY,
  order_id    TEXT NOT NULL UNIQUE,
  amount      BIGINT NOT NULL,
  status      TEXT NOT NULL,          -- COMPLETED
  pg_tx_id    TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Debezium Outbox Event Router의 기본 컬럼명(aggregatetype, aggregateid, type, payload)을 그대로 쓴다.
-- published_at 컬럼이 없다: "발행 여부"는 Debezium이 WAL 위치(LSN)로 추적하므로 테이블에 기록할 필요가 없다.
CREATE TABLE IF NOT EXISTS outbox (
  id             UUID PRIMARY KEY,      -- → Kafka 헤더 message_id (커넥터 설정의 additional.placement)
  aggregatetype  TEXT NOT NULL,         -- 'payment' → 토픽 'payment.events' 로 라우팅
  aggregateid    TEXT NOT NULL,         -- orderId → Kafka key
  type           TEXT NOT NULL,         -- 'PaymentCompleted' → 헤더 event_type
  payload        JSONB NOT NULL,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
