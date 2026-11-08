package statemachine

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Janitor 는 "결과를 모르는" 결제(UNKNOWN)와 "취소가 덜 끝난" 결제(CANCEL_REQUESTED)를 주기적으로 정리한다.
// 앱에서는 Run(ctx, interval) 이 주기 실행한다 (cmd/main.go). 테스트에서는 now 를 직접 넘겨 시간을 조종한다.
type Janitor struct {
	DB      *pgxpool.Pool
	Gateway *FakePaymentGateway

	MaxAttempts   int           // 이 횟수만큼 확인해도 결론이 안 나면 MANUAL_REVIEW
	BaseBackoff   time.Duration // 1차 재시도 간격. 이후 2배씩 (지수 백오프)
	NotFoundGrace time.Duration // PG 에 기록이 없어도 이 시간 동안은 "아직 반영 전일 수 있다"고 보고 기다린다
	PendingAfter  time.Duration // 이보다 오래된 PENDING 만 PG 에 조회한다 (0 이하면 SweepPending 이 거부)
	BatchSize     int
	PgCallTimeout time.Duration // PG 조회/취소 1회의 제한 시간 (0 이하면 5초)
}

// JanitorStats 는 한 번 실행한 결과.
type JanitorStats struct {
	Approved, Failed, ManualReview, Rescheduled, Canceled int
	ToUnknown                                             int // PENDING → UNKNOWN 으로 넘긴 건
}

// SweepPending 은 오래된 PENDING 을 정리한다.
// PENDING 을 커밋한 뒤 PG 응답을 기록하기 전에 프로세스가 죽으면
// 결제가 PENDING 에 영원히 남는다. PendingAfter 보다 오래된 것만 집어
// PG 에 묻는다 (진행 중인 요청과 경쟁하지 않게. killbill 의
// createdDateBefore cutoff 와 같은 역할).
//
//   - APPROVED / FAILED → 바로 확정
//   - NOT_FOUND → UNKNOWN 과 같은 grace 규칙. grace 가 지났으면
//     FAILED, 아니면 UNKNOWN 으로 넘겨 grace 끝에 다시 본다 (안 셈)
//   - 판단 불가(조회 장애) → UNKNOWN 으로 넘긴다 (1회로 셈).
//     재시도·MANUAL_REVIEW 는 ReconcileUnknown 하나가 맡는다.
func (j *Janitor) SweepPending(ctx context.Context, now time.Time) (JanitorStats, error) {
	var stats JanitorStats
	if j.PendingAfter <= 0 {
		// 0 이면 방금 PENDING 이 된 (PG 응답을 기다리는) 결제까지 집는다
		return stats, errors.New("janitor: PendingAfter must be > 0")
	}
	err := withTx(ctx, j.DB, func(tx pgx.Tx) error {
		// ReconcileUnknown 과 같은 claim. 고르는 조건만 다르다:
		// PENDING 엔 next_retry_at 이 없으므로 created_at 으로 고른다.
		// 이 tx 가 끝날 때까지 행 잠금을 쥐므로, 그 사이 요청의 CAS 는
		// 기다렸다가 0행(ErrStaleStatus)이 된다 → 여기선 Stale 이 없다.
		rows, err := tx.Query(ctx, `
			UPDATE payments SET attempt_count = attempt_count + 1
			 WHERE id IN (
			   SELECT id FROM payments
			    WHERE status = 'PENDING' AND created_at <= $1
			    ORDER BY created_at
			    LIMIT $2
			      FOR UPDATE SKIP LOCKED)
			RETURNING id::text, idempotency_key, attempt_count,
			          created_at`,
			now.Add(-j.PendingAfter), j.batchSize())
		if err != nil {
			return err
		}
		// 컬럼 모양이 같아 unknownRow 를 그대로 쓴다
		batch, err := pgx.CollectRows(rows, pgx.RowToStructByPos[unknownRow])
		if err != nil {
			return err
		}

		for _, p := range batch {
			status, pgTxID, qerr := j.getStatus(ctx, p.Key)
			switch {
			case qerr == nil && status == PgApproved:
				reason := "janitor: stale pending, pg says approved"
				if err := TransitionTx(ctx, tx, p.ID, Pending, Approved, reason); err != nil {
					return err
				}
				if _, err := tx.Exec(ctx, `
					UPDATE payments SET pg_tx_id = $2, next_retry_at = NULL
					 WHERE id = $1`, p.ID, pgTxID); err != nil {
					return err
				}
				stats.Approved++

			case qerr == nil && status == PgFailed:
				reason := "janitor: stale pending, pg says failed"
				if err := j.fail(ctx, tx, Pending, p.ID, reason); err != nil {
					return err
				}
				stats.Failed++

			case qerr == nil && status == PgNotFound:
				graceEnd := p.CreatedAt.Add(j.NotFoundGrace)
				if !now.Before(graceEnd) { // now >= graceEnd
					// PG 에 도달하기 전에 죽었다. 돈은 안 빠졌다.
					reason := "janitor: stale pending, no pg record after grace"
					if err := j.fail(ctx, tx, Pending, p.ID, reason); err != nil {
						return err
					}
					stats.Failed++
					continue
				}
				// UNKNOWN 규칙과 같다: grace 안의 NOT_FOUND 는 시도로
				// 세지 않고(-1) grace 끝 시각에 UNKNOWN 쪽이 다시 본다.
				err := j.pendingToUnknown(ctx, tx, p.ID,
					"pg not found (within grace)", graceEnd, p.Attempts-1)
				if err != nil {
					return err
				}
				stats.ToUnknown++

			default:
				// 결과를 모른다 = UNKNOWN. 이번 확인은 1회로 센다.
				reason := fmt.Sprintf("inconclusive (status=%s, err=%v)",
					status, qerr)
				err := j.pendingToUnknown(ctx, tx, p.ID, reason,
					now.Add(j.backoff(p.Attempts)), p.Attempts)
				if err != nil {
					return err
				}
				stats.ToUnknown++
			}
		}
		return nil
	})
	return stats, err
}

// pendingToUnknown: 이후 재시도/보류는 ReconcileUnknown 이 맡는다.
func (j *Janitor) pendingToUnknown(ctx context.Context, tx pgx.Tx,
	id, reason string, next time.Time, attempts int) error {
	reason = "janitor: stale pending - " + reason
	if err := TransitionTx(ctx, tx, id, Pending, Unknown, reason); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `
		UPDATE payments
		   SET attempt_count = $2, last_error = $3, next_retry_at = $4
		 WHERE id = $1`, id, attempts, reason, next)
	return err
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
			status, pgTxID, qerr := j.getStatus(ctx, p.Key)
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
				if err := j.fail(ctx, tx, Unknown, p.ID, "janitor: pg says failed"); err != nil {
					return err
				}
				stats.Failed++

			case qerr == nil && status == PgNotFound:
				graceEnd := p.CreatedAt.Add(j.NotFoundGrace)
				if !now.Before(graceEnd) { // now >= graceEnd
					// 충분히 기다렸는데도 PG 에 기록이 없다
					// → 요청이 PG 에 도달하지 못한 것. 돈은 안 빠졌다.
					reason := "janitor: pg has no record after grace"
					if err := j.fail(ctx, tx, Unknown, p.ID, reason); err != nil {
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

func (j *Janitor) fail(ctx context.Context, tx pgx.Tx, from Status, id, reason string) error {
	if err := TransitionTx(ctx, tx, id, from, Failed, reason); err != nil {
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
			if cerr := j.cancel(ctx, p.PgTxID); cerr != nil {
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

// getStatus / cancel 은 PG 호출 1회에 제한 시간을 건다.
// janitor 는 행 잠금을 쥔 채 PG 를 부른다 (Go 는 claim 과 처리가 한 tx).
// PG 가 응답하지 않으면 그 행을 기다리는 요청의 CAS 까지 같이 멈춘다.
// 시간이 지나면 ctx 가 취소돼 에러(context.DeadlineExceeded)가 돌아오고,
// 호출부는 다른 PG 장애와 똑같이 "판단 불가" 로 다룬다
// (조회: UNKNOWN 백오프/MANUAL_REVIEW, 취소: 재예약 — 취소는 멱등이라
// 실제로는 처리됐어도 다시 불러도 안전하다).
func (j *Janitor) getStatus(ctx context.Context, key string) (PgStatus, string, error) {
	ctx, cancel := context.WithTimeout(ctx, j.pgCallTimeout())
	defer cancel()
	return j.Gateway.GetStatus(ctx, key)
}

func (j *Janitor) cancel(ctx context.Context, pgTxID string) error {
	ctx, cancel := context.WithTimeout(ctx, j.pgCallTimeout())
	defer cancel()
	return j.Gateway.Cancel(ctx, pgTxID)
}

func (j *Janitor) pgCallTimeout() time.Duration {
	if j.PgCallTimeout <= 0 {
		return 5 * time.Second
	}
	return j.PgCallTimeout
}

func (j *Janitor) batchSize() int {
	if j.BatchSize <= 0 {
		return 100
	}
	return j.BatchSize
}

// Run 은 ctx 가 취소될 때까지 janitor 를 주기 실행한다.
// Kotlin 의 @Scheduled(fixedDelay = ...) 에 해당한다.
//
// fixed delay: "한 번 다 돌고 난 시점부터" interval 을 쉰다
// (03 OutboxRelay.Run 과 같은 방식). killbill 은
// scheduleAtFixedRate(시작 시각 기준)를 쓰지만, 여기선
// 한 바퀴가 PG 조회 때문에 느려져도 다음 바퀴가 바로 붙어
// 실행되지 않게 delay 를 고른다. 어차피 처리 대상은
// next_retry_at 으로 정해지므로 정확한 주기는 중요하지 않다.
//
// 에러가 나도 루프는 멈추지 않는다: 로그만 남기고 다음 주기에
// 다시 시도한다. (Spring 도 @Scheduled 메서드의 예외를 로그로
// 남기고 다음 실행을 계속한다)
func (j *Janitor) Run(ctx context.Context, interval time.Duration) {
	for {
		// 이미 취소됐으면 DB 를 건드리지 않고 바로 끝낸다
		if ctx.Err() != nil {
			return
		}
		j.runOnce(ctx, time.Now())
		// select: 먼저 준비되는 채널 쪽을 실행 (Kotlin 의 select { })
		select {
		case <-ctx.Done(): // 종료 신호 (cancel() / SIGTERM)
			return
		case <-time.After(interval): // 다 돈 시점부터 interval 뒤
		}
	}
}

// runOnce 는 한 바퀴: 오래된 PENDING 정리 → UNKNOWN 확정 → 취소 재시도.
// 한쪽이 실패해도 다른 쪽은 실행한다 (서로 독립적인 작업).
// PENDING 을 먼저 본다: 판단 불가로 UNKNOWN 에 넘긴 건은
// next_retry_at(백오프 뒤)이 정하는 바퀴에 UNKNOWN 쪽이 다시 본다.
func (j *Janitor) runOnce(ctx context.Context, now time.Time) {
	p, perr := j.SweepPending(ctx, now)
	if perr != nil && ctx.Err() == nil {
		log.Printf("janitor: sweep pending 실패"+
			" (다음 주기에 재시도): %v", perr)
	}
	u, uerr := j.ReconcileUnknown(ctx, now)
	if uerr != nil && ctx.Err() == nil {
		log.Printf("janitor: reconcile unknown 실패"+
			" (다음 주기에 재시도): %v", uerr)
	}
	c, cerr := j.RetryCancels(ctx, now)
	if cerr != nil && ctx.Err() == nil {
		log.Printf("janitor: retry cancels 실패"+
			" (다음 주기에 재시도): %v", cerr)
	}
	// 실행 요약. 할 일이 없던 바퀴는 조용히 넘어간다
	// (Go 표준 log 에는 DEBUG 레벨이 없다).
	idle := JanitorStats{}
	if p != idle || u != idle || c != idle {
		log.Printf("janitor run:"+
			" pending{approved=%d failed=%d toUnknown=%d}"+
			" unknown{approved=%d failed=%d"+
			" manualReview=%d rescheduled=%d}"+
			" cancel{canceled=%d rescheduled=%d}",
			p.Approved, p.Failed, p.ToUnknown,
			u.Approved, u.Failed, u.ManualReview, u.Rescheduled,
			c.Canceled, c.Rescheduled)
	}
}
