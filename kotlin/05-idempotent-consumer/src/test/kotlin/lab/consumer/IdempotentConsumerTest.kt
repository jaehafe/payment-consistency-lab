package lab.consumer

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.apache.kafka.common.TopicPartition
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.kafka.config.TopicBuilder
import org.springframework.transaction.support.TransactionTemplate
import java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeUnit

// 백그라운드 컨슈머를 끄고, 테스트가 ConsumerLoop.pollOnce()를 직접 돌린다.
@SpringBootTest(properties = ["lab.consumer.enabled=false"])
@Import(TestcontainersConfiguration::class)
class IdempotentConsumerTest {
    @Autowired lateinit var kafka: KafkaTemplate<String, String>
    @Autowired lateinit var consumerFactory: ConsumerFactory<String, String>
    @Autowired lateinit var jdbc: JdbcTemplate
    @Autowired lateinit var tx: TransactionTemplate
    @Autowired lateinit var objectMapper: ObjectMapper
    @Autowired lateinit var kafkaAdmin: KafkaAdmin

    private val openLoops = mutableListOf<ConsumerLoop>()
    private lateinit var orderId: String

    @BeforeEach
    fun setUp() {
        jdbc.execute("TRUNCATE orders, point_ledger, processed_messages")
        orderId = "order-" + UUID.randomUUID()
        jdbc.update("INSERT INTO orders (id, status) VALUES (?, 'PENDING')", orderId)
    }

    @AfterEach
    fun closeLoops() = openLoops.forEach { runCatching { it.close() } }

    @Test
    fun naive_consumer__duplicate_message__points_doubled() {
        val messageId = UUID.randomUUID().toString()
        // 03의 relay가 at-least-once라 같은 이벤트(같은 message_id)가 두 번 발행된 상황
        publish(messageId)
        publish(messageId)

        val loop = loop(newGroupId()) { NaiveConsumer(jdbc, tx) }
        pollUntil(loop) { it.count { d -> d.messageId == messageId } == 2 }

        assertEquals(2, pointRows(), "같은 결제에 포인트가 두 번 적립됐다")
        assertEquals(200L, pointSum())
        assertEquals("PAID" to 10_000L, orderState(), "덮어쓰기 갱신은 자연적으로 멱등이라 멀쩡하다 — 문제는 누적되는 부작용")
    }

    @Test
    fun idempotent_consumer__duplicate_message__processed_once() {
        val messageId = UUID.randomUUID().toString()
        publish(messageId)
        publish(messageId)

        val group = newGroupId()
        val loop = loop(group) { IdempotentConsumer(jdbc, tx, group) }
        pollUntil(loop) { it.count { d -> d.messageId == messageId } == 2 }

        val ours = loop.deliveries.filter { it.messageId == messageId }
        assertEquals(listOf(true, false), ours.map { it.processed }, "두 번 받았지만 두 번째는 건너뛰었다")
        assertEquals(1, pointRows())
        assertEquals(100L, pointSum())
        assertEquals("PAID" to 10_000L, orderState())
    }

    @Test
    fun idempotent_consumer__crash_before_offset_commit__redelivery_is_skipped() {
        val messageId = UUID.randomUUID().toString()
        publish(messageId) // 이번엔 한 번만 발행

        val group = newGroupId()

        // 1) 첫 번째 컨슈머: DB 처리는 커밋했는데 offset 커밋 직전에 죽는다
        val first = loop(group) { IdempotentConsumer(jdbc, tx, group) }.apply { crashBeforeOffsetCommit = true }
        pollUntil(first) { it.any { d -> d.messageId == messageId } }
        assertEquals(1, pointRows(), "DB 처리는 커밋됐다")
        first.close() // 프로세스 사망 — offset은 커밋되지 않았다

        // 2) 재시작한 컨슈머(같은 group): 마지막 커밋 offset부터 다시 받으므로 같은 메시지가 재전송된다
        val restarted = loop(group) { IdempotentConsumer(jdbc, tx, group) }
        pollUntil(restarted) { it.any { d -> d.messageId == messageId } }

        val redelivered = restarted.deliveries.single { it.messageId == messageId }
        assertEquals(false, redelivered.processed, "재전송은 processed_messages에 걸려 건너뛰었다")
        assertEquals(1, pointRows(), "포인트는 여전히 한 번만 적립")

        // 3) 건너뛴 뒤에도 offset은 커밋됐다 → 다음 재시작 때 또 받지 않는다 (= 그 메시지의 다음 위치)
        val tp = TopicPartition("payment.events", redelivered.partition)
        val committed = consumerFactory.createConsumer(group, null).use { it.committed(setOf(tp))[tp] }
        assertEquals(redelivered.offset + 1, committed?.offset(), "스킵한 메시지까지 offset 커밋")
    }

    @Test
    fun consumer_loop__missing_message_id_header__fails_with_clear_error() {
        // message_id 헤더 없이 발행된 메시지 (프로듀서 버그 등). 중복 제거 키가 없으니 처리할 수 없다.
        // 이 메시지가 공용 토픽에 남으면 다른 테스트의 컨슈머도 걸리므로 전용 토픽을 쓴다.
        val topic = "payment.events.missing-header-" + UUID.randomUUID()
        kafkaAdmin.createOrModifyTopics(TopicBuilder.name(topic).partitions(1).replicas(1).build())
        val event = PaymentCompletedEvent("pay-" + UUID.randomUUID(), orderId, 10_000, "pg_tx", Instant.now())
        kafka.send(ProducerRecord(topic, orderId, objectMapper.writeValueAsString(event))).get(10, TimeUnit.SECONDS)

        val group = newGroupId()
        val loop = ConsumerLoop(consumerFactory.createConsumer(group, null), topic, IdempotentConsumer(jdbc, tx, group), objectMapper)
            .also { openLoops += it }
        val deadline = Instant.now().plusSeconds(30)
        val error = assertThrows<IllegalStateException> {
            while (Instant.now() < deadline) loop.pollOnce()
        }
        assertTrue(error.message!!.contains("message_id"), "무엇이 빠졌는지 알려주는 메시지: ${error.message}")
        assertEquals(0, pointRows(), "처리하지 않았다")
    }

    // ---- helpers ----

    private fun publish(messageId: String) {
        val event = PaymentCompletedEvent("pay-" + UUID.randomUUID(), orderId, 10_000, "pg_tx", Instant.now())
        val record = ProducerRecord("payment.events", orderId, objectMapper.writeValueAsString(event))
        record.headers().add("message_id", messageId.toByteArray())
        kafka.send(record).get(10, TimeUnit.SECONDS)
    }

    private fun loop(groupId: String, handler: () -> MessageHandler): ConsumerLoop =
        ConsumerLoop(consumerFactory.createConsumer(groupId, null), "payment.events", handler(), objectMapper)
            .also { openLoops += it }

    /** 조건을 만족할 때까지 poll. 크래시 주입 예외는 "죽었다"는 표시일 뿐이므로 삼키고 계속 관찰한다. */
    private fun pollUntil(loop: ConsumerLoop, condition: (List<ConsumerLoop.Delivery>) -> Boolean) {
        val deadline = Instant.now().plusSeconds(30)
        while (!condition(loop.deliveries)) {
            check(Instant.now() < deadline) { "기대한 메시지를 30초 안에 받지 못함: ${loop.deliveries}" }
            try {
                loop.pollOnce()
            } catch (_: SimulatedCrashException) {
            }
        }
    }

    // 각 테스트가 새 group으로 토픽을 처음부터 읽는다 (다른 테스트의 offset에 영향받지 않도록)
    private fun newGroupId() = "point-service-" + UUID.randomUUID()

    private fun pointRows() =
        jdbc.queryForObject("SELECT count(*) FROM point_ledger WHERE order_id = ?", Int::class.java, orderId)!!

    private fun pointSum() =
        jdbc.queryForObject("SELECT coalesce(sum(points), 0) FROM point_ledger WHERE order_id = ?", Long::class.java, orderId)!!

    private fun orderState() = jdbc.queryForMap("SELECT status, paid_amount FROM orders WHERE id = ?", orderId)
        .let { it["status"] as String to (it["paid_amount"] as Number).toLong() }
}
