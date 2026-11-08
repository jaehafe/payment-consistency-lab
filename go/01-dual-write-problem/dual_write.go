package dualwrite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrDBCommitFailed 는 "커밋 직전에 DB가 실패했다"를 흉내 내는 에러.
// 실제로는 커넥션 끊김, 제약조건 위반, statement timeout, 페일오버 등으로 일어난다.
var ErrDBCommitFailed = errors.New("db commit failed (simulated)")

// DualWritePaymentService 는 "DB 저장"과 "Kafka 발행"을 각각 따로 하는 나이브 구현 3가지를 담는다.
// 어느 순서로 하든 둘 중 하나만 성공하는 순간이 생긴다 → 이것이 dual write 문제.
type DualWritePaymentService struct {
	DB        *pgxpool.Pool
	Gateway   *FakePaymentGateway
	Publisher EventPublisher

	// FailBeforeCommit 이 true 면 DB 트랜잭션을 커밋하지 않고 롤백한다 (실패 주입 스위치).
	FailBeforeCommit bool
}

// CommitThenPublish : DB 커밋 → Kafka 발행.
// 발행이 실패하면 DB에는 결제가 있는데 이벤트는 영영 없다 (이벤트 유실).
func (s *DualWritePaymentService) CommitThenPublish(ctx context.Context, orderID string, amount int64) (Payment, error) {
	p, err := s.charge(ctx, orderID, amount)
	// if err != nil { return ..., err } : Go의 표준 에러 처리. 예외 대신 매번 확인하고 위로 돌려준다.
	// Kotlin으로 치면 runCatching { }.getOrElse { return Result.failure(it) } 를 매 줄 쓰는 느낌.
	if err != nil {
		return Payment{}, err
	}

	// 1) DB 저장. 단일 INSERT 는 autocommit 이므로 이 줄이 끝나면 이미 커밋된 상태다.
	if err := insertPayment(ctx, s.DB, p); err != nil {
		return Payment{}, err
	}

	// 2) Kafka 발행. 여기서 실패해도 1)은 되돌릴 수 없다. ← 문제 지점
	if err := s.publish(ctx, p); err != nil {
		// fmt.Errorf 의 %w 는 원래 에러를 감싼다(wrap). 나중에 errors.Is(err, ErrBrokerDown) 로 원인을 확인할 수 있다.
		return p, fmt.Errorf("결제는 커밋됐지만 이벤트 발행 실패: %w", err)
	}
	return p, nil
}

// PublishThenCommit : Kafka 발행 → DB 커밋.
// 커밋이 실패하면 이벤트는 이미 나갔는데 DB엔 결제가 없다 (유령 이벤트).
func (s *DualWritePaymentService) PublishThenCommit(ctx context.Context, orderID string, amount int64) (Payment, error) {
	p, err := s.charge(ctx, orderID, amount)
	if err != nil {
		return Payment{}, err
	}

	// 1) 먼저 발행. 컨슈머들은 이 순간부터 "결제 완료"를 믿고 움직인다 (배송 시작, 포인트 적립…).
	if err := s.publish(ctx, p); err != nil {
		return Payment{}, err
	}

	// 2) DB 저장. 실패하면 1)에서 나간 메시지를 회수할 방법이 없다. ← 문제 지점
	if err := s.saveInTx(ctx, p); err != nil {
		return Payment{}, fmt.Errorf("이벤트는 발행됐지만 DB 커밋 실패: %w", err)
	}
	return p, nil
}

// PublishInsideTx : 트랜잭션 "안에서" 발행하면 원자적일 것 같지만 아니다.
// Kafka 는 DB 트랜잭션에 참여하지 않으므로, 이후 롤백돼도 메시지는 이미 브로커에 있다.
// (Spring의 @Transactional 안에서 kafkaTemplate.send() 를 호출하는 흔한 실수와 같은 구조)
func (s *DualWritePaymentService) PublishInsideTx(ctx context.Context, orderID string, amount int64) (Payment, error) {
	p, err := s.charge(ctx, orderID, amount)
	if err != nil {
		return Payment{}, err
	}

	// pgx 트랜잭션 사용법: Begin → (작업) → Commit. 중간에 return 하면 defer 된 Rollback 이 실행된다.
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return Payment{}, err
	}
	// Commit 이 이미 성공했다면 Rollback 은 아무 일도 하지 않는다(에러만 반환하고 무시됨).
	// 그래서 "Begin 직후 defer Rollback" 이 Go의 관용구다. Kotlin의 TransactionTemplate 이 예외 시 롤백해주는 것과 같은 효과.
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, insertPaymentSQL, p.ID, p.OrderID, p.Amount, p.Status, p.PgTxID); err != nil {
		return Payment{}, err
	}

	// 트랜잭션이 열려 있는 상태에서 발행 → 브로커에는 즉시 기록된다.
	if err := s.publish(ctx, p); err != nil {
		return Payment{}, err
	}

	// 발행 이후 단계(재고 차감, 다른 테이블 갱신, 커밋 자체 등)에서 실패했다고 하자.
	if s.FailBeforeCommit {
		return Payment{}, ErrDBCommitFailed // → defer 된 tx.Rollback 이 실행되어 INSERT 는 사라진다. 메시지는 남는다.
	}
	if err := tx.Commit(ctx); err != nil {
		return Payment{}, err
	}
	return p, nil
}

// ---- 이하 내부 헬퍼 (소문자로 시작 = 패키지 밖에서 안 보임) ----

const insertPaymentSQL = `INSERT INTO payments (id, order_id, amount, status, pg_tx_id) VALUES ($1, $2, $3, $4, $5)`

// charge 는 PG 승인 후 저장할 Payment 값을 만든다.
func (s *DualWritePaymentService) charge(ctx context.Context, orderID string, amount int64) (Payment, error) {
	pgTxID, err := s.Gateway.Charge(ctx, "order-"+orderID, orderID, amount)
	if err != nil {
		return Payment{}, err
	}
	return Payment{ID: uuid.NewString(), OrderID: orderID, Amount: amount, Status: "COMPLETED", PgTxID: pgTxID}, nil
}

func (s *DualWritePaymentService) publish(ctx context.Context, p Payment) error {
	ev := PaymentCompleted{PaymentID: p.ID, OrderID: p.OrderID, Amount: p.Amount, PgTxID: p.PgTxID, OccurredAt: time.Now().UTC()}
	payload, err := json.Marshal(ev) // Kotlin의 objectMapper.writeValueAsBytes(ev)
	if err != nil {
		return err
	}
	return s.Publisher.Publish(ctx, p.OrderID, uuid.NewString(), payload)
}

// saveInTx 는 트랜잭션으로 저장하되, FailBeforeCommit 이면 커밋 대신 롤백한다.
func (s *DualWritePaymentService) saveInTx(ctx context.Context, p Payment) error {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, insertPaymentSQL, p.ID, p.OrderID, p.Amount, p.Status, p.PgTxID); err != nil {
		return err
	}
	if s.FailBeforeCommit {
		return ErrDBCommitFailed
	}
	return tx.Commit(ctx)
}

// insertPayment 는 pool 에서 바로 실행한다 (= 단일 문장 autocommit).
func insertPayment(ctx context.Context, db *pgxpool.Pool, p Payment) error {
	_, err := db.Exec(ctx, insertPaymentSQL, p.ID, p.OrderID, p.Amount, p.Status, p.PgTxID)
	return err
}

// FindPaymentByOrderID 는 테스트 검증용 조회. 없으면 (Payment{}, false, nil).
func FindPaymentByOrderID(ctx context.Context, db *pgxpool.Pool, orderID string) (Payment, bool, error) {
	var p Payment
	// QueryRow(...).Scan(&a, &b) : 한 행을 읽어 변수들의 주소(&)에 채워 넣는다.
	err := db.QueryRow(ctx,
		`SELECT id::text, order_id, amount, status, coalesce(pg_tx_id, '') FROM payments WHERE order_id = $1`, orderID).
		Scan(&p.ID, &p.OrderID, &p.Amount, &p.Status, &p.PgTxID)
	// errors.Is 는 감싼(wrap) 에러까지 따라가며 같은 에러인지 비교한다. Kotlin의 `is` 타입 검사와 비슷한 용도.
	if errors.Is(err, pgx.ErrNoRows) {
		return Payment{}, false, nil
	}
	if err != nil {
		return Payment{}, false, err
	}
	return p, true, nil
}
