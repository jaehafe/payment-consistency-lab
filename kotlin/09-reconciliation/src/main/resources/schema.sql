-- 이 단계의 테이블은 스키마 k09에 만든다. 커넥션의 search_path 는 application.yml 의
-- spring.datasource.hikari.schema=k09 가 맞춰 두므로 아래 DDL에는 스키마 접두어가 없다.
CREATE SCHEMA IF NOT EXISTS k09;

-- 우리 시스템이 기록한 거래 (08의 transactions를 대사용으로 간소화)
CREATE TABLE IF NOT EXISTS internal_transactions (
  id          UUID PRIMARY KEY,
  pg_tx_id    TEXT NOT NULL,           -- 의도적으로 UNIQUE 아님: 내부 중복도 탐지 대상
  order_id    TEXT NOT NULL,
  type        TEXT NOT NULL,           -- PAYMENT | REFUND
  amount      BIGINT NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS recon_files (             -- PG 정산 파일 import 이력 (같은 파일 두 번 import 방지)
  id         BIGSERIAL PRIMARY KEY,
  file_name  TEXT NOT NULL,
  file_hash  TEXT NOT NULL UNIQUE,     -- SHA-256 (파일명이 아니라 "내용"으로 판단)
  row_count  INT NOT NULL,
  imported_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS pg_settlement_records (
  id          BIGSERIAL PRIMARY KEY,
  file_id     BIGINT NOT NULL REFERENCES recon_files(id),
  pg_tx_id    TEXT NOT NULL,
  order_id    TEXT NOT NULL,
  type        TEXT NOT NULL,
  amount      BIGINT NOT NULL,
  fee         BIGINT NOT NULL,
  transacted_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS recon_results (
  id              BIGSERIAL PRIMARY KEY,
  run_id          UUID NOT NULL,
  business_date   DATE NOT NULL,
  kind            TEXT NOT NULL,   -- MISSING_IN_INTERNAL | MISSING_IN_PG | AMOUNT_MISMATCH | DUPLICATE_IN_PG | DUPLICATE_IN_INTERNAL
  pg_tx_id        TEXT NOT NULL,
  internal_amount BIGINT,
  pg_amount       BIGINT,
  diff_amount     BIGINT,          -- pg_amount - internal_amount (+면 PG가 더 많이 앎, -면 우리가 더 많이 앎)
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
