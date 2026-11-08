-- 이 단계의 테이블은 스키마 k01에 만든다. 커넥션의 search_path 는 application.yml 의
-- spring.datasource.hikari.schema=k01 가 맞춰 두므로 아래 DDL에는 스키마 접두어가 없다.
CREATE SCHEMA IF NOT EXISTS k01;

-- spring.sql.init.mode=always 로 앱 기동 시 실행된다.
CREATE TABLE IF NOT EXISTS payments (
  id          UUID PRIMARY KEY,
  order_id    TEXT NOT NULL UNIQUE,
  amount      BIGINT NOT NULL,
  status      TEXT NOT NULL,          -- COMPLETED
  pg_tx_id    TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
