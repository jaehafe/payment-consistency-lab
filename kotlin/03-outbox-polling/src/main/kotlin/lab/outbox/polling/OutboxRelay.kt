package lab.outbox.polling

import org.springframework.stereotype.Component
import org.springframework.transaction.support.TransactionTemplate

/** relay가 "발행은 했는데 published_at 커밋 전에 죽은" 상황을 흉내내는 예외 */
class SimulatedCrashException : RuntimeException("장애 주입: 발행 후, published_at 커밋 전에 프로세스 사망")

/**
 * Polling Relay: outbox에서 미발행 행을 읽어 Kafka로 보내고 published_at을 찍는다.
 *
 * 보장 수준은 at-least-once(최소 한 번)다.
 * "발행"과 "published_at 커밋"은 여전히 서로 다른 두 시스템에 대한 쓰기이므로,
 * 그 사이에서 죽으면 다음 relay 실행이 같은 행을 다시 발행한다 → 중복 가능.
 * 반대 순서(먼저 mark, 나중에 발행)로 하면 at-most-once(유실 가능)가 된다. 결제 이벤트는 유실보다 중복이 낫다.
 * 중복은 컨슈머가 message_id로 걸러낸다 (05-idempotent-consumer).
 */
@Component
class OutboxRelay(
    private val tx: TransactionTemplate,
    private val outbox: OutboxRepository,
    private val publisher: OutboxPublisher,
) {
    /** true면 발행 직후, 커밋 전에 죽는다 (at-least-once 증명용) */
    @Volatile var crashAfterPublish = false

    /** @return 이번에 발행한 건수 */
    fun relayOnce(batchSize: Int): Int = tx.execute {
        // 1) 미발행 행을 잠그고 가져온다 (FOR UPDATE SKIP LOCKED — 다른 relay와 겹치지 않음)
        val batch = outbox.lockUnpublishedBatch(batchSize)

        // 2) 순서대로 동기 발행. 중간에 실패하면 예외 → 트랜잭션 롤백 → 이 배치 전체가 다음 회차에 재시도된다.
        //    (실패 전까지 이미 나간 메시지는 다시 나간다 = 중복. at-least-once의 또 다른 예)
        //    트레이드오프: 발행하는 동안 DB 트랜잭션(행 잠금)을 열어 두므로 배치를 너무 크게 잡지 않는다.
        for (m in batch) publisher.publish(m)

        if (crashAfterPublish) throw SimulatedCrashException()

        // 3) 발행 확인(ack)을 받은 것만 published_at 기록 → 트랜잭션 커밋과 함께 잠금 해제
        outbox.markPublished(batch.map { it.id })
        batch.size
    }!!
}
