-- 04-outbox-cdc 스키마 (docs/SPEC.md 와 동일)
CREATE TABLE IF NOT EXISTS payments (
  id          UUID PRIMARY KEY,
  order_id    TEXT NOT NULL UNIQUE,
  amount      BIGINT NOT NULL,
  status      TEXT NOT NULL,          -- COMPLETED
  pg_tx_id    TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Debezium Outbox Event Router 의 기본 컬럼명(aggregatetype, aggregateid, type, payload)을 그대로 쓴다.
-- 이렇게 하면 커넥터 설정(infra/debezium/outbox-connector.json)에서 컬럼 매핑을 따로 적을 필요가 거의 없다.
-- published_at 이 없다: "발행 여부"는 Debezium 이 WAL 위치(offset)로 관리한다.
CREATE TABLE IF NOT EXISTS outbox (
  id             UUID PRIMARY KEY,
  aggregatetype  TEXT NOT NULL,
  aggregateid    TEXT NOT NULL,
  type           TEXT NOT NULL,
  payload        JSONB NOT NULL,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
