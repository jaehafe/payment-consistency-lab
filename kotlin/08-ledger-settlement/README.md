# 08. 입금·출금·수수료·정산 (복식부기 원장)

## 문제

결제 대행(PG/마켓플레이스) 플랫폼에서 돈은 이렇게 흐른다:
고객 결제 → PG가 나중에 우리 통장으로 입금 → 우리가 수수료를 떼고 가맹점에 지급.

`merchant.balance += amount` 같은 단식 잔액 갱신으로는:
- "이 잔액이 왜 이 값인가"를 설명할 수 없고,
- 버그로 돈이 새거나 두 번 들어가도 알아챌 방법이 없고,
- 수수료 반올림, 부분 환불, 날짜 경계 같은 디테일에서 1원씩 어긋난다.

## 해결

**복식부기**: 모든 거래를 "어느 계정에서 어느 계정으로"의 분개(차변 = 대변)로 기록한다. 돈은 생기거나 사라지지 않고 이동만 하므로
모든 계정 잔액(차변 +, 대변 -)의 합은 **항상 0**이다. 0이 아니면 어딘가 버그다.

| 거래 | 차변 (Dr) | 대변 (Cr) |
|---|---|---|
| 결제 승인 10,000원 (수수료 3%) | `pg:receivable` 10,000 | `merchant:m1:payable` 9,700 / `platform:fee_revenue` 300 |
| 환불 | 결제의 역분개 (`payable`·`fee_revenue` 차변, `receivable` 대변) | |
| PG 정산금 입금 | `bank:cash` | `pg:receivable` |
| 가맹점 지급 | `merchant:m1:payable` | `bank:cash` |

- **금액은 전부 원 단위 정수.** 수수료 = `(amount × bps + 5000) / 10000` — 정수만으로 HALF_UP 반올림. (`1050 × 0.03`을 Double로 하면 `31.499999...`)
- **원장 멱등성**: `journal_entries UNIQUE (reference_type, reference_id)` → 같은 결제가 두 번 와도(05의 중복 메시지) 한 번만 전기.
- **append-only**: 환불·정정은 기존 분개를 고치지 않고 새 분개로.
- **정산**: KST 거래일 `[00:00, 다음날 00:00)` 범위의 미정산 거래를 가맹점별 집계. `net = gross - refund - (결제 수수료 - 환불 수수료 환급)`. 거래에 `settlement_id`를 찍고, `UNIQUE (merchant_id, settlement_date)`로 재실행 중복 방지.

## 핵심 코드 위치

- `src/main/kotlin/lab/ledger/Money.kt:13` — 정수 수수료 반올림, `:21` KST 하루 범위
- `src/main/kotlin/lab/ledger/LedgerService.kt:22` — `post`: 검증(`:60`, 차변≠대변 거부 `:68`), 멱등 insert(`:34`), 계정 id 순서 갱신(`:41`)
- `src/main/kotlin/lab/ledger/TransactionService.kt:25` — 결제 분개, `:51` 환불(역분개), `:70` PG 입금
- `src/main/kotlin/lab/ledger/SettlementService.kt:37` — `settle`: 대상 행 잠금(`:46`), UNIQUE 방어(`:77`), settlement_id 기록(`:89`)
- `src/main/kotlin/lab/ledger/SettlementService.kt:100` — `payout`: 상태 CAS + 지급 분개
- `src/main/kotlin/lab/ledger/ReportQueries.kt:20` — 가맹점별 일일 리포트, `:46` 시산표(캐시 잔액 vs 라인 합계)

## 테스트 실행

```bash
cd kotlin
./gradlew :08-ledger-settlement:test
```

| 테스트 | 증명하는 것 |
|---|---|
| `payment_posting__debits_equal_credits` | 10,000 = 9,700 + 300, 계정별 잔액 |
| `unbalanced_entry__rejected` | 차변 1,000 / 대변 900 → 거부, 아무것도 기록 안 됨 |
| `fee_rounding__half_up_in_won` | 31.5 → 32, 31.47 → 31, 1.5 → 2 |
| `same_reference_posted_twice__single_entry` | 같은 pg_tx_id 두 번 → 분개 1건 |
| `refund__reverses_payment_entries` | 환불 라인 = 결제 라인의 차/대변 반전, 잔액 0 |
| `settle__net_equals_gross_minus_refund_minus_fee` | 30,000 - 5,000 - 750 = 24,250 = 원장의 가맹점 미지급금 |
| `settle__uses_kst_day_boundary` | UTC 15:30 = KST 다음날 00:30 → 다음날 정산 |
| `settle_twice_same_date__no_duplicate_settlement` | 재실행 시 새 정산 0건 |
| `payout__merchant_payable_zero_after_payout` | 지급 후 미지급금 0, 통장 잔액 = 수수료 수익, 이중 지급 불가 |
| `trial_balance__sum_of_balances_is_zero` | 시산표 합계 0, 캐시 잔액 = 라인 합계 |

시산표 쿼리 (직접 실행용):
```sql
SELECT a.id, a.type, a.balance, sum(l.debit) - sum(l.credit) AS lines_balance
FROM ledger_accounts a LEFT JOIN ledger_lines l ON l.account_id = a.id
GROUP BY a.id, a.type, a.balance ORDER BY a.id;
-- 그리고: SELECT sum(balance) FROM ledger_accounts;  → 0 이어야 한다
```

## 한계 / 다음 단계

- PG가 자기 수수료를 떼고 입금하는 경우(실제로 흔함) `pg:fee_expense` 계정이 추가로 필요하다. 여기선 PG가 전액 입금한다고 단순화.
- 이미 정산된 날짜에 늦게 도착한 거래는 미정산으로 남긴다(로그 경고). 실무에선 다음 정산 주기로 이월하거나 정정 정산을 만든다.
- net이 음수(환불이 더 많은 날)인 정산은 지급하지 않는다. 다음 정산과 상계하거나 가맹점에 청구해야 한다.
- 원장 숫자가 내부적으로 맞아도 PG가 아는 숫자와 다를 수 있다 → **09 대사**.
