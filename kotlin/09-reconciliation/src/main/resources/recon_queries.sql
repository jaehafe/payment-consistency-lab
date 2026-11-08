-- =====================================================================
-- 대사(reconciliation) 쿼리 모음
--
-- ReconciliationService가 "-- name: xxx" 단위로 잘라서 실행한다.
-- psql에서 직접 실행해 볼 수도 있다 (psql도 :변수 문법을 쓴다):
--
--   psql -h localhost -p 15432 -U lab lab -v business_date="'2026-11-07'"
--   lab=> \i recon_queries.sql            -- 전부 실행
--   (또는 아래 쿼리 하나를 복사해 붙여넣기)
--
-- 대상 기간: KST 영업일 business_date 하루 = [그날 00:00 KST, 다음날 00:00 KST)
--   date::timestamp AT TIME ZONE 'Asia/Seoul' → "KST 벽시계 시각"을 timestamptz(절대 시각)로 변환
-- 차액(diff_amount) 부호: pg_amount - internal_amount
-- =====================================================================


-- name: discrepancies
-- 누락 / 금액 차이: 양쪽을 pg_tx_id로 FULL OUTER JOIN 한다.
--   왼쪽만 있음  → MISSING_IN_PG       (우리는 결제됐다는데 PG는 모름 → 고객 돈 안 받았을 수 있음)
--   오른쪽만 있음 → MISSING_IN_INTERNAL (PG는 승인했는데 우리는 모름 → 07의 UNKNOWN을 잘못 FAILED 처리 등)
--   둘 다 있는데 금액 다름 → AMOUNT_MISMATCH
-- 중복 행은 아래 duplicate_* 쿼리가 따로 잡으므로, 여기서는 pg_tx_id별로 한 줄로 접는다(min).
WITH day AS (
  SELECT (CAST(:business_date AS date))::timestamp AT TIME ZONE 'Asia/Seoul'     AS from_ts,
         (CAST(:business_date AS date) + 1)::timestamp AT TIME ZONE 'Asia/Seoul' AS to_ts
),
internal AS (
  SELECT t.pg_tx_id, min(t.amount) AS amount
  FROM internal_transactions t, day
  WHERE t.occurred_at >= day.from_ts AND t.occurred_at < day.to_ts
  GROUP BY t.pg_tx_id
),
pg AS (
  SELECT r.pg_tx_id, min(r.amount) AS amount
  FROM pg_settlement_records r, day
  WHERE r.transacted_at >= day.from_ts AND r.transacted_at < day.to_ts
  GROUP BY r.pg_tx_id
)
SELECT CASE
         WHEN i.pg_tx_id IS NULL THEN 'MISSING_IN_INTERNAL'
         WHEN p.pg_tx_id IS NULL THEN 'MISSING_IN_PG'
         ELSE 'AMOUNT_MISMATCH'
       END                                         AS kind,
       coalesce(p.pg_tx_id, i.pg_tx_id)            AS pg_tx_id,
       i.amount                                    AS internal_amount,
       p.amount                                    AS pg_amount,
       coalesce(p.amount, 0) - coalesce(i.amount, 0) AS diff_amount
FROM internal i
FULL OUTER JOIN pg p ON p.pg_tx_id = i.pg_tx_id
WHERE i.pg_tx_id IS NULL
   OR p.pg_tx_id IS NULL
   OR i.amount <> p.amount
ORDER BY kind, pg_tx_id;


-- name: duplicates_in_pg
-- PG 파일 안에서 같은 pg_tx_id가 여러 번 → PG가 두 번 정산(입금)하려는 것. 초과분 = 더 받을 돈(+).
WITH day AS (
  SELECT (CAST(:business_date AS date))::timestamp AT TIME ZONE 'Asia/Seoul'     AS from_ts,
         (CAST(:business_date AS date) + 1)::timestamp AT TIME ZONE 'Asia/Seoul' AS to_ts
)
SELECT 'DUPLICATE_IN_PG'                 AS kind,
       r.pg_tx_id,
       NULL::bigint                      AS internal_amount,
       sum(r.amount)                     AS pg_amount,
       sum(r.amount) - min(r.amount)     AS diff_amount,   -- 한 건을 정상으로 보고, 나머지가 초과분
       count(*)                          AS occurrences
FROM pg_settlement_records r, day
WHERE r.transacted_at >= day.from_ts AND r.transacted_at < day.to_ts
GROUP BY r.pg_tx_id
HAVING count(*) > 1
ORDER BY r.pg_tx_id;


-- name: duplicates_in_internal
-- 우리 쪽에 같은 pg_tx_id가 여러 번 → 이중 기록(05의 멱등 컨슈머 부재 등). 초과분 = 우리가 더 안다고 착각하는 돈(-).
WITH day AS (
  SELECT (CAST(:business_date AS date))::timestamp AT TIME ZONE 'Asia/Seoul'     AS from_ts,
         (CAST(:business_date AS date) + 1)::timestamp AT TIME ZONE 'Asia/Seoul' AS to_ts
)
SELECT 'DUPLICATE_IN_INTERNAL'               AS kind,
       t.pg_tx_id,
       sum(t.amount)                         AS internal_amount,
       NULL::bigint                          AS pg_amount,
       -(sum(t.amount) - min(t.amount))      AS diff_amount,
       count(*)                              AS occurrences
FROM internal_transactions t, day
WHERE t.occurred_at >= day.from_ts AND t.occurred_at < day.to_ts
GROUP BY t.pg_tx_id
HAVING count(*) > 1
ORDER BY t.pg_tx_id;


-- name: matched_count
-- 양쪽에 다 있고 금액도 같은 건수 (리포트의 "일치")
WITH day AS (
  SELECT (CAST(:business_date AS date))::timestamp AT TIME ZONE 'Asia/Seoul'     AS from_ts,
         (CAST(:business_date AS date) + 1)::timestamp AT TIME ZONE 'Asia/Seoul' AS to_ts
),
internal AS (
  SELECT t.pg_tx_id, min(t.amount) AS amount FROM internal_transactions t, day
  WHERE t.occurred_at >= day.from_ts AND t.occurred_at < day.to_ts GROUP BY t.pg_tx_id
),
pg AS (
  SELECT r.pg_tx_id, min(r.amount) AS amount FROM pg_settlement_records r, day
  WHERE r.transacted_at >= day.from_ts AND r.transacted_at < day.to_ts GROUP BY r.pg_tx_id
)
SELECT count(*) AS matched
FROM internal i JOIN pg p ON p.pg_tx_id = i.pg_tx_id AND p.amount = i.amount;


-- name: latest_run_summary
-- 운영용: 해당 영업일 가장 최근 대사 실행의 종류별 건수와 차액 합계
SELECT kind, count(*) AS cnt, sum(diff_amount) AS diff_sum
FROM recon_results
WHERE run_id = (
  SELECT run_id FROM recon_results
  WHERE business_date = CAST(:business_date AS date)
  ORDER BY created_at DESC, id DESC LIMIT 1
)
GROUP BY kind
ORDER BY kind;
