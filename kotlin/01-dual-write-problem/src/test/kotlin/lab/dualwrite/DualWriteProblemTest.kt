package lab.dualwrite

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.dao.DataAccessException
import org.springframework.kafka.KafkaException
import org.testcontainers.redpanda.RedpandaContainer
import java.util.UUID

/**
 * 이중 쓰기의 세 가지 실패 모드를 실제 Postgres + Redpanda로 재현한다.
 * 각 테스트는 "DB 상태"와 "브로커에 실제로 기록된 이벤트"를 각각 확인해서 둘이 어긋남을 보인다.
 */
@SpringBootTest
@Import(TestcontainersConfiguration::class)
class DualWriteProblemTest {
    @Autowired lateinit var service: DualWritePaymentService
    @Autowired lateinit var payments: PaymentRepository
    @Autowired lateinit var publisher: PaymentEventPublisher
    @Autowired lateinit var dbFailure: DbFailureInjector
    @Autowired lateinit var redpanda: RedpandaContainer

    private val probe by lazy { EventProbe(redpanda.bootstrapServers, "payment.events") }

    @AfterEach
    fun resetToggles() {
        publisher.brokerDown = false
        dbFailure.failBeforeCommit = false
    }

    @Test
    fun commit_then_publish__broker_failure__payment_saved_but_event_lost() {
        val orderId = newOrderId()
        publisher.brokerDown = true

        assertThrows<KafkaException> { service.commitThenPublish(orderId, 10_000) }

        assertEquals(1, payments.countByOrderId(orderId), "DB에는 결제가 커밋돼 있다")
        assertEquals(0, probe.eventsFor(orderId).size, "그런데 PaymentCompleted 이벤트는 브로커에 없다 → 다운스트림은 결제를 모른다")
    }

    @Test
    fun publish_then_commit__db_failure__ghost_event_published() {
        val orderId = newOrderId()
        dbFailure.failBeforeCommit = true

        assertThrows<DataAccessException> { service.publishThenCommit(orderId, 10_000) }

        assertEquals(0, payments.countByOrderId(orderId), "DB는 롤백돼 결제가 없다")
        val events = probe.eventsFor(orderId)
        assertEquals(1, events.size, "그런데 이벤트는 이미 나갔다 → 존재하지 않는 결제에 대한 '유령 이벤트'")
    }

    @Test
    fun publish_inside_tx__rollback__event_already_sent() {
        val orderId = newOrderId()
        dbFailure.failBeforeCommit = true // 발행 이후, 커밋 직전에 실패 → 롤백

        assertThrows<DataAccessException> { service.publishInsideTx(orderId, 10_000) }

        assertEquals(0, payments.countByOrderId(orderId), "트랜잭션은 롤백됐다")
        assertEquals(1, probe.eventsFor(orderId).size, "Kafka는 DB 트랜잭션에 참여하지 않으므로 이벤트는 롤백되지 않는다")
    }

    private fun newOrderId() = "order-" + UUID.randomUUID()
}
