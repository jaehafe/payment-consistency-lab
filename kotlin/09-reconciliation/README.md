# 09. 대사 (누락 · 중복 · 금액 차이)

## 문제

멱등키(02), outbox(03), 멱등 컨슈머(05), 상태 머신(07), 원장(08)을 다 갖춰도 "이론상" 맞을 뿐이다.
버그, 장애, 사람의 수동 처리, PG 쪽 오류는 언제나 있다. 그리고 그 결과는 **돈이 맞지 않는 것**이다.
- PG는 승인했는데 우리는 모른다 (07에서 UNKNOWN을 잘못 FAILED로 처리)
- 우리는 결제됐다는데 PG는 모른다
- 금액이 다르다 (부분 취소 반영 누락)
- PG 파일에 같은 거래가 두 번 / 우리 쪽에 같은 거래가 두 번 (05의 중복 처리)

## 해결

매일 PG가 보내 주는 **정산 파일**(외부의 진실)과 우리 기록을 맞춰 본다.

1. **파일 import**: CSV(`pg_tx_id,order_id,type,amount,fee,transacted_at`) → `pg_settlement_records`. 파일 **내용의 SHA-256**을 `recon_files.file_hash UNIQUE`로 기록해 같은 파일을 두 번 넣지 않는다 (넣으면 전 건이 DUPLICATE_IN_PG가 된다).
2. **대사 SQL** (`src/main/resources/recon_queries.sql`, 사람이 직접 실행 가능):
   - `FULL OUTER JOIN ... ON pg_tx_id` → 한쪽만 있으면 **누락**, 둘 다 있는데 금액이 다르면 **금액 차이**
   - `GROUP BY pg_tx_id HAVING count(*) > 1` → **중복**
   - 대상: KST 영업일 하루 `[00:00, 다음날 00:00)` (`date::timestamp AT TIME ZONE 'Asia/Seoul'`)
3. **결과 저장 + 리포트**: `recon_results`에 run_id 단위로 쌓고(이전 실행을 덮지 않음), 종류별 건수/차액 합계/일치 건수를 Markdown 리포트로.

| 종류 | 의미 | 차액(PG - 내부) |
|---|---|---|
| `MISSING_IN_INTERNAL` | PG만 앎 → 돈은 받았는데 주문 처리 안 됨 | + |
| `MISSING_IN_PG` | 우리만 앎 → 상품은 나갔는데 돈을 못 받을 수 있음 | - |
| `AMOUNT_MISMATCH` | 금액 다름 | ± |
| `DUPLICATE_IN_PG` | PG 파일에 중복 → 초과 정산분 | + |
| `DUPLICATE_IN_INTERNAL` | 우리 쪽 이중 기록 | - |

## 핵심 코드 위치

- `src/main/resources/recon_queries.sql:17` — `discrepancies` (FULL OUTER JOIN `:49`)
- `src/main/resources/recon_queries.sql:56` / `:75` — 중복 탐지 (HAVING `:71`, `:90`)
- `src/main/resources/recon_queries.sql:94` — 일치 건수, `:112` 운영용 최근 실행 요약
- `src/main/kotlin/lab/recon/ReconQueries.kt:19` — `-- name:` 단위로 SQL 파일 파싱
- `src/main/kotlin/lab/recon/PgSettlementFileImporter.kt:26` — 해시 기반 중복 import 방지(`:37`)
- `src/main/kotlin/lab/recon/ReconciliationService.kt:66` — `run`, `:34` Markdown 리포트
- `src/test/resources/fixtures/pg_settlement_2026-11-07.csv` — 모든 불일치 종류가 들어 있는 PG 파일 fixture

## SQL 직접 실행

```bash
cd infra && docker compose up -d postgres
cd ../kotlin && ./gradlew :09-reconciliation:bootRun     # 테이블 생성 (곧바로 종료됨)
psql -h localhost -p 15432 -U lab lab -v business_date="'2026-11-07'"
lab=> \i 09-reconciliation/src/main/resources/recon_queries.sql
```

psql도 `:변수` 문법을 쓰므로 앱이 쓰는 SQL 파일을 그대로 실행할 수 있다.

## 테스트 실행

```bash
cd kotlin
./gradlew :09-reconciliation:test
```

시나리오는 `ReconciliationTest` 상단 표 참고. KST 경계 검증용으로 UTC 15:30(= KST 다음날 00:30) 거래를 넣어 두었고, 11-07 대사 대상에서 빠진다.

| 테스트 | 증명하는 것 |
|---|---|
| `matched_records__no_discrepancy` | 결제+환불이 모두 일치 → 불일치 0, 차액 0 |
| `missing_in_internal__detected` | PG만 아는 12,000원 → +12,000 |
| `missing_in_pg__detected` | 우리만 아는 15,000원 → -15,000 |
| `amount_mismatch__detected_with_diff` | 30,000 vs 29,000 → -1,000 |
| `duplicate_in_pg_file__detected` | PG 파일 2줄 → 초과분 +7,000 |
| `duplicate_internal__detected` | 내부 2건 → -5,000 |
| `same_file_imported_twice__ignored` | 이름만 바꾼 같은 파일 → 무시, 레코드 8건 유지 |
| `report__summarizes_counts_and_diff_amount` | 일치 4, 종류별 1건씩, 차액 합계 -2,000, 결과 5행 저장, Markdown |

## 한계 / 다음 단계

- **자정 경계 시차**: 우리는 23:59:59에 기록했는데 PG는 00:00:01로 찍으면 양쪽 날짜에서 각각 "누락"이 된다. 실무에선 전날/다음날 미매칭 건을 한 번 더 맞춰 보는 단계(또는 ±N분 허용)를 둔다.
- `pg_tx_id`만으로 맞춘다. 부분 취소가 같은 pg_tx_id를 공유하는 PG라면 (pg_tx_id, type, 순번) 같은 복합 키가 필요하다.
- 탐지까지만 한다. 해소(보정 분개, 수동 처리 기록, 해소 상태 관리)는 별도 워크플로가 필요하다.
- 대사가 매일 0건이어도 그것이 "실시간으로" 맞다는 뜻은 아니다 → **10 관측성**으로 이상을 그때그때 감지.
