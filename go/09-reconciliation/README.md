# 09. 대사 (Reconciliation) — 누락·중복·금액 차이

## 문제

01~08 의 모든 장치(멱등키, outbox, 멱등 컨슈머, 상태 머신, janitor)를 둬도 현실에서는 숫자가 어긋난다.
- PG 는 승인했는데 우리 DB 에는 없음 (UNKNOWN 처리 누락, 버그)
- 우리는 승인으로 기록했는데 PG 정산 파일에는 없음 (PG 쪽 취소, 정산 주기 차이)
- 금액이 다름 (부분 취소 반영 누락, 통화/할인 버그)
- 같은 거래가 두 번 (PG 파일 중복, 우리 쪽 이벤트 이중 처리)

**대사**는 매일 PG 정산 파일과 우리 거래 기록을 한 줄씩 맞춰보고, 어긋난 것을 사람이 확인할 목록으로 만드는 마지막 안전망이다.

## 해결

1. PG 정산 CSV(`pg_tx_id,order_id,type,amount,fee,transacted_at`)를 `pg_settlement_records` 로 import.
   파일 내용의 **SHA-256** 을 `recon_files.file_hash UNIQUE` 로 기록 → 같은 파일이 다시 와도(이름이 바뀌어도) 무시.
2. `recon_queries.sql` 의 이름 붙은 쿼리로 비교 (대상 기간 = KST business_date):
   - `compare_by_pg_tx_id`: `FULL OUTER JOIN ... ON pg_tx_id` → 한쪽에만 있으면 MISSING_*, 금액이 다르면 AMOUNT_MISMATCH
   - `duplicate_in_pg` / `duplicate_in_internal`: `GROUP BY pg_tx_id HAVING count(*) > 1`
3. 결과를 `recon_results` 에 저장하고 `ReconReport`(종류별 건수, 차액 합계, 일치 건수) + Markdown 리포트를 만든다.

**SQL 은 `recon_queries.sql` 에 따로 있다.** Go 코드를 몰라도 이 파일만 읽고 psql 에서 `$1` 을 `'2026-11-08'` 로 바꿔 바로 실행해볼 수 있다.

| kind | 의미 | diff_amount (= PG - 내부) | 흔한 원인 / 조치 |
|---|---|---|---|
| `MISSING_IN_INTERNAL` | PG 에만 있음 | + PG 금액 | UNKNOWN 미처리, 콜백 유실 → 07 janitor 확인, 내부 기록 보정 |
| `MISSING_IN_PG` | 우리에게만 있음 | - 내부 금액 | PG 측 취소, 정산 주기 차이(익일 반영) → 다음 날 재대사, PG 문의 |
| `AMOUNT_MISMATCH` | 금액 다름 | PG - 내부 | 부분 취소 누락, 금액 계산 버그 |
| `DUPLICATE_IN_PG` | PG 파일에 중복 | + 초과분 | PG 파일 오류 → PG 문의 (과지급분은 나중에 회수됨) |
| `DUPLICATE_IN_INTERNAL` | 우리 DB 에 중복 | - 초과분 | 멱등성 누락(05) → 중복 행 정정, 원장(08) 역분개 |

## 핵심 코드 위치

- `recon_queries.sql:18` `compare_by_pg_tx_id` — `:26,32` `DISTINCT ON` (중복이 JOIN 결과를 부풀리지 않게), `:48` `FULL OUTER JOIN`
- `recon_queries.sql:51` `duplicate_in_pg`, `:65` `duplicate_in_internal` — `HAVING count(*) > 1`
- `queries.go:12` `//go:embed recon_queries.sql`, `:20` `-- name:` 파서
- `importer.go:39` `Import` — `:40` SHA-256, `:58` 같은 파일 무시, `:72` `CopyFrom`(COPY 프로토콜 대량 insert), `:93` `encoding/csv` 파싱
- `service.go:55` `Run`, `service.go:105` `Markdown`
- `testdata/pg_settlement_2026-11-08.csv` — PG 정산 파일 fixture

## 테스트 실행

```bash
cd go
go test ./09-reconciliation/ -v     # 마지막 테스트가 Markdown 리포트를 출력한다
```

| 테스트 | 증명하는 것 |
|---|---|
| `Test_matched_records__no_discrepancy` | 2건 일치, 불일치 0, recon_results 0행 |
| `Test_missing_in_internal__detected` | PG 에만 있는 10,000원 → diff +10,000 |
| `Test_missing_in_pg__detected` | 내부에만 있는 10,000원 → diff -10,000 |
| `Test_amount_mismatch__detected_with_diff` | 내부 10,000 vs PG 9,500 → diff -500 |
| `Test_duplicate_in_pg_file__detected` | PG 파일에 같은 거래 2줄 → 초과분 +10,000, 금액 비교는 1건으로 일치 |
| `Test_duplicate_internal__detected` | 내부에 같은 거래 2줄 → 초과분 -10,000 |
| `Test_same_file_imported_twice__ignored` | 이름만 바꾼 같은 파일 → skip, 레코드 7행 그대로 |
| `Test_report__summarizes_counts_and_diff_amount` | fixture 로 5종류 각 1건, 일치 4건, 차액 합 -13,000 / 절대값 합 45,000, 전날 거래 제외 |

## 한계 / 다음 단계

- **날짜 경계**: 23:59:59 거래가 우리는 오늘, PG 는 내일로 잡을 수 있다. 실무에서는 MISSING 건을 전후일 데이터와 한 번 더 맞춰보거나 1~2일 지연 대사를 한다.
- 지금은 `pg_tx_id` + 금액만 비교한다. 거래 유형(PAYMENT/REFUND), 수수료(`fee`), 주문 ID 불일치도 같은 방식으로 확장할 수 있다.
- 쿼리의 `(시각 AT TIME ZONE 'Asia/Seoul')::date = $1` 은 읽기 쉽지만 인덱스를 못 탄다. 데이터가 크면 KST 자정 범위 조건(`>= 시작 AND < 끝`)으로 바꾼다.
- 대사는 **탐지**까지다. 보정(내부 기록 추가, 역분개)은 사람이 확인한 뒤 별도 절차로 한다. 자동 보정은 잘못되면 피해를 키운다.
