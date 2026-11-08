package lab.dualwrite

import org.springframework.stereotype.Service
import org.springframework.transaction.support.TransactionTemplate
import java.util.UUID

/**
 * [안티패턴] 나이브한 이중 쓰기(dual write) 구현 3가지.
 *
 * DB(Postgres)와 브로커(Kafka)는 서로 다른 시스템이고, 둘을 묶는 분산 트랜잭션이 없다.
 * 그래서 "둘 다 성공하거나 둘 다 실패"를 보장할 방법이 없고, 어떤 순서로 하든 한쪽만 성공하는 틈이 생긴다.
 * 각 메서드의 틈은 DualWriteProblemTest가 실제 Postgres + Redpanda로 증명한다.
 */
@Service
class DualWritePaymentService(
    private val pg: PaymentGateway,
    private val payments: PaymentRepository,
    private val publisher: PaymentEventPublisher,
    private val tx: TransactionTemplate,
    private val dbFailure: DbFailureInjector,
) {
    /** 1) DB 커밋 → 발행. 발행이 실패하면: DB엔 결제가 있는데 이벤트는 영영 안 나간다 (이벤트 유실). */
    fun commitThenPublish(orderId: String, amount: Long): Payment {
        val payment = charge(orderId, amount)

        tx.executeWithoutResult {
            payments.insert(payment)
            dbFailure.maybeFail()
        } // ← 여기서 COMMIT. 이후 무슨 일이 생겨도 결제 데이터는 남는다.

        // 커밋과 발행 사이에 프로세스가 죽거나 브로커가 죽으면 이벤트는 사라진다.
        // 재시도를 넣어도 "재시도 도중 프로세스가 죽는" 경우는 막을 수 없다.
        publisher.publish(PaymentCompletedEvent.from(payment))
        return payment
    }

    /** 2) 발행 → DB 커밋. 커밋이 실패하면: 이벤트는 나갔는데 결제는 없다 (유령 이벤트). */
    fun publishThenCommit(orderId: String, amount: Long): Payment {
        val payment = charge(orderId, amount)

        // 먼저 발행 → 컨슈머들(배송, 포인트, 알림…)은 이미 "결제 완료"를 믿고 움직이기 시작한다.
        publisher.publish(PaymentCompletedEvent.from(payment))

        tx.executeWithoutResult {
            payments.insert(payment)
            dbFailure.maybeFail() // 여기서 실패하면 롤백되지만, 이미 나간 이벤트는 회수할 수 없다.
        }
        return payment
    }

    /**
     * 3) 트랜잭션 "안에서" 발행. 언뜻 원자적으로 보이지만 아니다.
     * Kafka 프로듀서는 DB 트랜잭션에 참여하지 않는다 → DB가 롤백돼도 이벤트는 이미 브로커에 있다.
     * (@Transactional 메서드 안에서 kafkaTemplate.send()를 부르는 흔한 코드가 바로 이 경우)
     */
    fun publishInsideTx(orderId: String, amount: Long): Payment {
        val payment = charge(orderId, amount)

        tx.executeWithoutResult {
            payments.insert(payment)
            publisher.publish(PaymentCompletedEvent.from(payment)) // 브로커는 즉시 기록한다
            dbFailure.maybeFail() // 발행 이후의 어떤 실패든 → ROLLBACK. 하지만 발행은 롤백 대상이 아니다.
        }
        return payment
    }

    // PG 승인은 트랜잭션 밖에서 호출한다 (외부 호출 동안 DB 커넥션/락을 잡고 있지 않기 위해).
    // 이 단계에서는 orderId를 PG idempotency key로 쓴다.
    private fun charge(orderId: String, amount: Long): Payment {
        val pgTxId = pg.charge(idempotencyKey = orderId, orderId = orderId, amount = amount)
        return Payment(UUID.randomUUID(), orderId, amount, "COMPLETED", pgTxId)
    }
}
