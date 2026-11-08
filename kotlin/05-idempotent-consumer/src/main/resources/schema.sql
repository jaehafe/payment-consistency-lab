-- 이 단계의 테이블은 스키마 k05에 만든다. 커넥션의 search_path 는 application.yml 의
-- spring.datasource.hikari.schema=k05 가 맞춰 두므로 아래 DDL에는 스키마 접두어가 없다.
CREATE SCHEMA IF NOT EXISTS k05;

-- 이 단계는 결제 서비스가 아니라 payment.events를 "받는 쪽"(주문/포인트 서비스)이다.
CREATE TABLE IF NOT EXISTS orders (
  id          TEXT PRIMARY KEY,
  status      TEXT NOT NULL,      -- PENDING | PAID
  paid_amount BIGINT NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS point_ledger (     -- 결제 금액의 1% 적립. 중복 처리되면 두 번 쌓인다 (자연적으로 멱등하지 않은 부작용)
  id          BIGSERIAL PRIMARY KEY,
  order_id    TEXT NOT NULL,
  points      BIGINT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS processed_messages (
  consumer_group TEXT NOT NULL,
  message_id     TEXT NOT NULL,
  processed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer_group, message_id)   -- 이 PK가 "한 그룹에서 한 메시지는 한 번만"을 DB 차원에서 강제한다
);
