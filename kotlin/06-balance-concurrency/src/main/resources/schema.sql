-- 이 단계의 테이블은 스키마 k06에 만든다. 커넥션의 search_path 는 application.yml 의
-- spring.datasource.hikari.schema=k06 가 맞춰 두므로 아래 DDL에는 스키마 접두어가 없다.
CREATE SCHEMA IF NOT EXISTS k06;

CREATE TABLE IF NOT EXISTS accounts (
  id       TEXT PRIMARY KEY,
  balance  BIGINT NOT NULL,          -- 의도적으로 CHECK(balance >= 0) 없음: 나이브 구현의 초과 인출을 보이기 위해
  version  BIGINT NOT NULL DEFAULT 0 -- 낙관적 락용 버전
);
CREATE TABLE IF NOT EXISTS withdrawals (
  id          BIGSERIAL PRIMARY KEY,
  account_id  TEXT NOT NULL,
  amount      BIGINT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
