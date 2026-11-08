# 08. 입금·출금·수수료·정산 (복식부기 원장)

## 문제

- `merchants.balance += 9700` 처럼 잔액 컬럼 하나만 고치면, 나중에 "이 돈이 어디서 와서 어디로 갔는지" 설명할 수 없다. 버그로 숫자가 틀어지면 찾을 방법도 없다.
- 수수료를 `amount * 0.03` (float)으로 계산하면 `1050 * 0.03 = 31.499999...` → 반올림이 틀린다.
- 정산 배치가 두 번 돌면 가맹점에 두 번 지급한다.
- 같은 결제 이벤트가 두 번 오면(05 의 at-least-once) 원장에 두 번 쌓인다.
- "11월 8일 거래"를 UTC 로 자르면 KST 00:00~08:59 거래가 전날로 간다.

## 해결

**복식부기**: 모든 거래를 "차변(Dr) 합계 = 대변(Cr) 합계" 인 분개로 기록한다. 돈은 생기거나 사라지지 않고 계정 사이를 옮겨갈 뿐이다.
잔액은 debit-positive 규칙(`balance += debit - credit`)으로 갱신하므로, **모든 계정 잔액의 합은 항상 0** (시산표 검증).

| 거래 | 분개 |
|---|---|
| 결제 승인 10,000원 (수수료 3% = 300) | Dr `pg:receivable` 10,000 / Cr `merchant:m1:payable` 9,700 / Cr `platform:fee_revenue` 300 |
| 환불 | 결제 분개의 역분개 (차변 ↔ 대변), 환불분 수수료 환급 |
| PG 정산금 입금 | Dr `bank:cash` / Cr `pg:receivable` |
| 가맹점 지급 | Dr `merchant:m1:payable` / Cr `bank:cash` → settlement `PAID` |

10,000원 결제 → 입금 → 지급까지 끝나면: `pg:receivable 0`, `merchant payable 0`, `bank:cash +300`, `fee_revenue -300`. 은행에 남은 300원이 정확히 우리 수수료.

- **수수료**: `fee = (amount × rateBps + 5000) / 10000` — 정수만으로 원 단위 HALF_UP.
- **원장 멱등성**: `journal_entries UNIQUE(reference_type, reference_id)` + `ON CONFLICT DO NOTHING`.
- **정산 멱등성**: 정산된 거래에 `settlement_id` 를 찍고, `settlements UNIQUE(merchant_id, settlement_date)`.
- **KST 경계**: `time.LoadLocation("Asia/Seoul")` 로 KST 자정 ~ 다음 자정 범위를 만들어 `occurred_at >= start AND < end`.

## 핵심 코드 위치

- `ledger.go:58` `Validate` — 차변 합 ≠ 대변 합이면 거부
- `ledger.go:96` `PostTx` — `:106` 참조 중복 방지, `:118` 계정 ID 순 정렬(데드락 방지, 06 참고), `:132` debit-positive 잔액 갱신
- `transactions.go:21` `CalculateFee` (HALF_UP), `:91` `PaymentEntry`, `:103` `RefundEntry`(역분개)
- `settlement.go:31` `KSTDayRange`, `settlement.go:57` `Settle` — `:71` 대상 거래 잠금, `:78-81` `FILTER` 집계, `:105` 정산 중복 방지
- `settlement.go:128` `RecordPgDeposit`, `:143` `Payout` (분개 + PAID 를 한 트랜잭션)
- `report.go:44` `DailyReport` — 가맹점별 gross/refund/fee/net + 시산표

## 테스트 실행

```bash
cd go
go test ./08-ledger-settlement/ -v
```

| 테스트 | 증명하는 것 |
|---|---|
| `Test_payment_posting__debits_equal_credits` | 차변 10,000 = 대변 10,000, 계정 잔액 +10,000 / -9,700 / -300 |
| `Test_unbalanced_entry__rejected` | 1,000원이 빈 분개 → 거부, 아무것도 기록 안 됨 |
| `Test_fee_rounding__half_up_in_won` | 1,050→32, 1,049→31, 1,017→31, 1,016→30, 50→2 … (테이블 테스트) |
| `Test_same_reference_posted_twice__single_entry` | 같은 PG 거래 두 번 → 분개 1건, 잔액 1회 반영 |
| `Test_refund__reverses_payment_entries` | 전액 환불 → 모든 계정 0, 3줄 모두 차/대변 반전 |
| `Test_settle__net_equals_gross_minus_refund_minus_fee` | net 12,658 = 15,050 - 2,000 - 392, 원장의 가맹점 부채와 일치 |
| `Test_settle__uses_kst_day_boundary` | UTC 11/7 15:30(=KST 11/8 00:30) 은 11/8 정산, UTC 14:59 는 11/7 정산 |
| `Test_settle_twice_same_date__no_duplicate_settlement` | 재실행 시 새 정산 0건 |
| `Test_payout__merchant_payable_zero_after_payout` | 지급 후 가맹점 부채 0, 현금 300 = 수수료 수익, 이중 지급 거부 |
| `Test_trial_balance__sum_of_balances_is_zero` | 결제·환불·정산·입금·지급 후 잔액 합 0, 차변 합 = 대변 합 (`-v` 로 시산표 출력) |

## 한계 / 다음 단계

- 이미 정산된 날짜에 **늦게 도착한 거래**는 `UNIQUE(merchant_id, settlement_date)` 때문에 그날 정산에 못 들어간다. 실무에서는 "다음 정산에 포함" 또는 추가 정산 회차(`settlement_seq`)를 둔다.
- 환불이 결제보다 많으면 net 이 음수가 된다. 지금은 지급을 건너뛰고 PAID 처리하지만, 실무에서는 다음 정산과 상계하거나 가맹점에 청구한다.
- `ledger_accounts.balance` 갱신은 계정 행에 잠금을 건다. 수수료 수익 같은 핫 계정은 동시 결제가 많으면 병목 → 잔액은 비동기 집계하고 `ledger_lines` 만 append 하는 설계도 있다.
- PG 입금액과 우리가 계산한 금액이 다를 수 있다 → [09-reconciliation](../09-reconciliation).
