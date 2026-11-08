-- =============================================================================
-- 09-reconciliation 대사(reconciliation) 쿼리 모음
--
-- 이 파일은 Go 코드가 //go:embed 로 그대로 읽어서 실행한다 (queries.go).
-- "-- name: xxx" 줄이 쿼리 이름이고, 다음 "-- name:" 전까지가 그 쿼리 본문이다.
--
-- $1 = business_date (KST 기준 거래일, 예: '2026-11-08')
-- psql 에서 직접 실행할 때는 $1 을 '2026-11-08' 같은 날짜 리터럴로 바꾸면 된다.
--
-- 모든 쿼리는 같은 모양의 컬럼을 돌려준다:
--   kind, pg_tx_id, internal_amount, pg_amount, diff_amount (= pg - internal)
--
-- 날짜 조건: (시각 AT TIME ZONE 'Asia/Seoul')::date
--   DB 에는 UTC 로 저장돼 있으므로 KST 로 바꾼 뒤 날짜를 자른다. UTC 날짜로 자르면 KST 00:00~08:59 거래가 전날로 간다.
--   (읽기 쉬운 형태다. 데이터가 크면 인덱스를 타도록 "시각 >= 시작 AND 시각 < 끝" 범위 조건으로 바꾼다.)
-- =============================================================================

-- name: compare_by_pg_tx_id
-- 누락(MISSING_*)과 금액 차이(AMOUNT_MISMATCH)를 한 번에 찾는다.
-- FULL OUTER JOIN: 양쪽 어느 한쪽에만 있는 행도 남긴다 (없는 쪽 컬럼은 NULL).
--   i.pg_tx_id IS NULL → PG 에만 있음 = 우리 DB 에서 누락 (승인은 됐는데 우리가 기록 못함: 타임아웃/UNKNOWN 미처리 의심)
--   p.pg_tx_id IS NULL → 우리 DB 에만 있음 = PG 파일에서 누락 (PG 가 취소했거나, 정산 주기 차이)
-- DISTINCT ON (pg_tx_id): 중복 행은 아래 duplicate_* 쿼리가 따로 잡으므로, 여기서는 한 건만 남겨 비교한다.
--   (중복을 그대로 JOIN 하면 행이 곱해져서 결과가 부풀려진다)
WITH i AS (
  SELECT DISTINCT ON (pg_tx_id) pg_tx_id, amount
    FROM internal_transactions
   WHERE (occurred_at AT TIME ZONE 'Asia/Seoul')::date = $1::date
   ORDER BY pg_tx_id, occurred_at
),
p AS (
  SELECT DISTINCT ON (pg_tx_id) pg_tx_id, amount
    FROM pg_settlement_records
   WHERE (transacted_at AT TIME ZONE 'Asia/Seoul')::date = $1::date
   ORDER BY pg_tx_id, id
)
SELECT CASE
         WHEN i.pg_tx_id IS NULL THEN 'MISSING_IN_INTERNAL'
         WHEN p.pg_tx_id IS NULL THEN 'MISSING_IN_PG'
         WHEN i.amount <> p.amount THEN 'AMOUNT_MISMATCH'
         ELSE 'MATCHED'
       END                                       AS kind,
       coalesce(i.pg_tx_id, p.pg_tx_id)          AS pg_tx_id,
       i.amount                                  AS internal_amount,
       p.amount                                  AS pg_amount,
       coalesce(p.amount, 0) - coalesce(i.amount, 0) AS diff_amount
  FROM i
  FULL OUTER JOIN p ON i.pg_tx_id = p.pg_tx_id
 ORDER BY 2;

-- name: duplicate_in_pg
-- PG 파일 안에서 같은 pg_tx_id 가 여러 번 → PG 가 이중으로 정산금을 잡았을 수 있음 (우리에겐 이득이지만 나중에 회수됨)
-- diff_amount = 초과분 (합계 - 한 건 금액). sum(bigint) 의 결과는 numeric 이라 ::bigint 로 되돌린다.
SELECT 'DUPLICATE_IN_PG'          AS kind,
       pg_tx_id,
       NULL::bigint               AS internal_amount,
       min(amount)                AS pg_amount,
       (sum(amount) - min(amount))::bigint AS diff_amount
  FROM pg_settlement_records
 WHERE (transacted_at AT TIME ZONE 'Asia/Seoul')::date = $1::date
 GROUP BY pg_tx_id
HAVING count(*) > 1
 ORDER BY pg_tx_id;

-- name: duplicate_in_internal
-- 우리 DB 안에서 같은 pg_tx_id 가 여러 번 → 이벤트 중복 처리(멱등성 누락) 의심. 매출이 부풀려진다.
SELECT 'DUPLICATE_IN_INTERNAL'    AS kind,
       pg_tx_id,
       min(amount)                AS internal_amount,
       NULL::bigint               AS pg_amount,
       (-(sum(amount) - min(amount)))::bigint AS diff_amount
  FROM internal_transactions
 WHERE (occurred_at AT TIME ZONE 'Asia/Seoul')::date = $1::date
 GROUP BY pg_tx_id
HAVING count(*) > 1
 ORDER BY pg_tx_id;

-- name: results_by_run
-- (운영용) 특정 대사 실행 결과 조회. $1 = run_id
SELECT kind, pg_tx_id, internal_amount, pg_amount, diff_amount
  FROM recon_results
 WHERE run_id = $1::uuid
 ORDER BY kind, pg_tx_id;
