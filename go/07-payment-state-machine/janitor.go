package statemachine

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Janitor 는 "결과를 모르는" 결제(UNKNOWN)와 "취소가 덜 끝난" 결제(CANCEL_REQUESTED)를 주기적으로 정리한다.
// 실제로는 cron / @Scheduled 로 1분마다 돌린다. 테스트에서는 now 를 직접 넘겨 시간을 조종한다.
type Janitor struct {
	DB      *pgxpool.Pool
	Gateway *FakePaymentGateway

	MaxAttempts   int           // 이 횟수만큼 확인해도 결론이 안 나면 MANUAL_REVIEW
	BaseBackoff   time.Duration // 1차 재시도 간격. 이후 2배씩 (지수 백오프)
	NotFoundGrace time.Duration // PG 에 기록이 없어도 이 시간 동안은 "아직 반영 전일 수 있다"고 보고 기다린다
	BatchSize     int
}

// JanitorStats 는 한 번 실행한 결과.
type JanitorStats struct {
	Approved, Failed, ManualReview, Rescheduled, Canceled int
}

// maxBackoffShift 는 지수의 상한 (Kotlin 의 coerceAtMost(20) 과 같다).
// 1분 << 20 ≈ 2년. 이 이상은 의미도 없고 int64 오버플로만 난다.
const maxBackoffShift = 20

// backoff 는 n 번째 시도 후 기다릴 시간: Base, 2·Base, 4·Base, ...
// time.Duration 은 int64(나노초)라서 << (비트 시프트)로
// 2의 거듭제곱을 곱할 수 있다. 단, 상한이 없으면 1분 << 29 부터
// int64 를 넘어 음수가 된다 → 지수를 maxBackoffShift 로 자른다.
func (j *Janitor) backoff(attempt int) time.Duration {
	// min 은 Go 1.21+ 내장 함수 (Kotlin 의 coerceAtMost)
	shift := min(max(attempt-1, 0), maxBackoffShift)
	// Base 가 아주 크면(약 2.4시간 이상) << 20 도 넘친다 → 최댓값에서 멈춤
	if j.BaseBackoff > time.Duration(math.MaxInt64>>shift) {
		return time.Duration(math.MaxInt64)
	}
	return j.BaseBackoff << shift
}

// unknownRow 는 janitor 가 집어 온 UNKNOWN 결제.
type unknownRow struct {
	ID, Key   string
	Attempts  int
	CreatedAt time.Time
}

// ReconcileUnknown 은 재시도 시각이 된 UNKNOWN 결제를 PG 에 조회해서 확정한다.
func (j *Janitor) ReconcileUnknown(ctx context.Context, now time.Time) (JanitorStats, error) {
	var stats JanitorStats
	err := withTx(ctx, j.DB, func(tx pgx.Tx) error {
		// FOR UPDATE SKIP LOCKED: janitor 를 여러 인스턴스 띄워도 같은 결제를 동시에 처리하지 않는다 (03 단계 릴레이와 같은 기법).
		// 잠금은 이 트랜잭션 동안 유지된다. 그 사이 PG "조회"만 하므로(상태 변경 없음) 외부 호출을 tx 안에 둬도 괜찮다.
		// 집으면서 attempt_count 를 1 올린다 (= 이번 확인도 1회로 센다).
		// RETURNING 은 올린 뒤의 값을 돌려준다. Kotlin claimDue 와 같은 규칙.
		rows, err := tx.Query(ctx, `
			UPDATE payments SET attempt_count = attempt_count + 1
			 WHERE id IN (
			   SELECT id FROM payments
			    WHERE status = 'UNKNOWN' AND next_retry_at <= $1
			    ORDER BY next_retry_at
			    LIMIT $2
			      FOR UPDATE SKIP LOCKED)
			RETURNING id::text, idempotency_key, attempt_count,
			          created_at`, now, j.batchSize())
		if err != nil {
			return err
		}
		// pgx.RowToStructByPos: 컬럼 순서대로 구조체 필드에 채운다 (간단한 RowMapper)
		batch, err := pgx.CollectRows(rows, pgx.RowToStructByPos[unknownRow])
		if err != nil {
			return err
		}

		for _, p := range batch {
			status, pgTxID, qerr := j.Gateway.GetStatus(ctx, p.Key)
			switch {
			case qerr == nil && status == PgApproved:
				if err := TransitionTx(ctx, tx, p.ID, Unknown, Approved, "janitor: pg says approved"); err != nil {
					return err
				}
				if _, err := tx.Exec(ctx, `UPDATE payments SET pg_tx_id = $2, next_retry_at = NULL WHERE id = $1`, p.ID, pgTxID); err != nil {
					return err
				}
				stats.Approved++

			case qerr == nil && status == PgFailed:
				if err := j.fail(ctx, tx, p.ID, "janitor: pg says failed"); err != nil {
					return err
				}
				stats.Failed++

			case qerr == nil && status == PgNotFound:
				graceEnd := p.CreatedAt.Add(j.NotFoundGrace)
				if !now.Before(graceEnd) { // now >= graceEnd
					// 충분히 기다렸는데도 PG 에 기록이 없다
					// → 요청이 PG 에 도달하지 못한 것. 돈은 안 빠졌다.
					reason := "janitor: pg has no record after grace"
					if err := j.fail(ctx, tx, p.ID, reason); err != nil {
						return err
					}
					stats.Failed++
					continue
				}
				// grace 안의 NOT_FOUND 는 "실패한 확인"이 아니라
				// "아직 판단할 때가 아님"이다. 그래서 attempt_count 를
				// 쓰지 않고, grace 가 끝나는 시각으로 바로 재예약한다.
				// (시도 횟수를 쓰면 백오프 합 < grace 일 때 grace 가
				//  끝나기도 전에 MANUAL_REVIEW 로 가 버린다)
				// attempt_count - 1: 집을 때 올린 1 을 되돌린다.
				if _, err := tx.Exec(ctx, `
					UPDATE payments
					   SET attempt_count = attempt_count - 1,
					       last_error = $2, next_retry_at = $3
					 WHERE id = $1`,
					p.ID, "janitor: pg not found (within grace)",
					graceEnd); err != nil {
					return err
				}
				stats.Rescheduled++

			default:
				// 아직 결론을 못 냄 (조회 API 장애 등)
				reason := fmt.Sprintf("janitor: inconclusive (status=%s, err=%v)", status, qerr)
				attempts := p.Attempts // 집을 때 이번 시도까지 센 값
				if attempts >= j.MaxAttempts {
					// 자동 확인 포기 → 보류. 사람이 PG 어드민에서 확인해 처리한다 (README 의 운영 쿼리 참고).
					if err := TransitionTx(ctx, tx, p.ID, Unknown, ManualReview, reason); err != nil {
						return err
					}
					if _, err := tx.Exec(ctx,
						`UPDATE payments SET attempt_count = $2, last_error = $3, next_retry_at = NULL WHERE id = $1`,
						p.ID, attempts, reason); err != nil {
						return err
					}
					stats.ManualReview++
					continue // for 루프의 다음 반복으로
				}
				if _, err := tx.Exec(ctx,
					`UPDATE payments SET attempt_count = $2, last_error = $3, next_retry_at = $4 WHERE id = $1`,
					p.ID, attempts, reason, now.Add(j.backoff(attempts))); err != nil {
					return err
				}
				stats.Rescheduled++
			}
		}
		return nil
	})
	return stats, err
}

func (j *Janitor) fail(ctx context.Context, tx pgx.Tx, id, reason string) error {
	if err := TransitionTx(ctx, tx, id, Unknown, Failed, reason); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `UPDATE payments SET last_error = $2, next_retry_at = NULL WHERE id = $1`, id, reason)
	return err
}

// cancelRow 는 취소 재시도 대상.
type cancelRow struct {
	ID, PgTxID string
	Attempts   int
}

// RetryCancels 는 보상(취소)이 실패해 CANCEL_REQUESTED 에 남은 결제의 PG 취소를 다시 시도한다.
func (j *Janitor) RetryCancels(ctx context.Context, now time.Time) (JanitorStats, error) {
	var stats JanitorStats
	err := withTx(ctx, j.DB, func(tx pgx.Tx) error {
		// UNKNOWN 과 같이, 집으면서 attempt_count 를 1 올린다.
		rows, err := tx.Query(ctx, `
			UPDATE payments SET attempt_count = attempt_count + 1
			 WHERE id IN (
			   SELECT id FROM payments
			    WHERE status = 'CANCEL_REQUESTED'
			      AND next_retry_at <= $1
			    ORDER BY next_retry_at
			    LIMIT $2
			      FOR UPDATE SKIP LOCKED)
			RETURNING id::text, pg_tx_id, attempt_count`,
			now, j.batchSize())
		if err != nil {
			return err
		}
		batch, err := pgx.CollectRows(rows, pgx.RowToStructByPos[cancelRow])
		if err != nil {
			return err
		}
		for _, p := range batch {
			// PG 취소는 상태를 바꾸는 호출이지만 멱등하다(이미 취소된 건은 성공 처리). 그래서 재시도해도 안전하다.
			if cerr := j.Gateway.Cancel(ctx, p.PgTxID); cerr != nil {
				attempts := p.Attempts // 집을 때 이미 +1
				// 취소는 "포기"할 수 없다 (돈을 돌려줘야 하므로). 백오프하며 계속 재시도하고, 오래된 건은 알림으로 사람을 부른다.
				if _, err := tx.Exec(ctx,
					`UPDATE payments SET attempt_count = $2, last_error = $3, next_retry_at = $4 WHERE id = $1`,
					p.ID, attempts, cerr.Error(), now.Add(j.backoff(attempts))); err != nil {
					return err
				}
				stats.Rescheduled++
				continue
			}
			if err := TransitionTx(ctx, tx, p.ID, CancelRequested, Canceled, "janitor: pg cancel succeeded"); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `UPDATE payments SET next_retry_at = NULL WHERE id = $1`, p.ID); err != nil {
				return err
			}
			stats.Canceled++
		}
		return nil
	})
	return stats, err
}

func (j *Janitor) batchSize() int {
	if j.BatchSize <= 0 {
		return 100
	}
	return j.BatchSize
}
