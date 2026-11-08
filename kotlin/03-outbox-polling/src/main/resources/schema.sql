-- 이 단계의 테이블은 스키마 k03에 만든다. 커넥션의 search_path 는 application.yml 의
-- spring.datasource.hikari.schema=k03 가 맞춰 두므로 아래 DDL에는 스키마 접두어가 없다.
CREATE SCHEMA IF NOT EXISTS k03;

CREATE TABLE IF NOT EXISTS payments (
  id          UUID PRIMARY KEY,
  order_id    TEXT NOT NULL UNIQUE,
  amount      BIGINT NOT NULL,
  status      TEXT NOT NULL,          -- COMPLETED
  pg_tx_id    TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 테이블 이름이 SPEC의 `outbox`가 아니라 `outbox_polling`인 이유:
-- 단계별 스키마(k03)가 생기기 전에는 03과 04가 같은 lab DB의 public 스키마를 썼고, 04의 Debezium 커넥터가 보는
-- `public.outbox`(Debezium 기본 컬럼명)와 이름이 겹치면 먼저 만든 쪽 컬럼이 남아 04의 insert가 깨졌다.
-- 지금은 k03.outbox 와 public.outbox 가 다른 테이블이라 겹치지 않지만, 테이블 하나만 보고도 "polling용 outbox"
-- (published_at 있음)와 "CDC용 outbox"(없음)를 구분할 수 있게 이름은 그대로 둔다. 컬럼은 SPEC과 같다.
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
