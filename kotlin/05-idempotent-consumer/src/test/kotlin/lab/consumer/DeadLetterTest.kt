package lab.consumer

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.jdbc.datasource.DriverManagerDataSource
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.transaction.support.TransactionTemplate
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/** 테스트에서 주입하는 처리 실패 */
class InjectedFailure : RuntimeException("handler failure (injected)")

/**
 * DLT 테스트. ConsumerRunner를 실제로 돌린다 — Kotlin에서 "재시도"는
 * 컨슈머를 새로 만들어 마지막 커밋 지점부터 다시 받는 것이라서,
 * loopFactory 호출 횟수 = 그 메시지를 시도한 횟수가 된다.
 */
@SpringBootTest(properties = ["lab.consumer.enabled=false"])
@Import(TestcontainersConfiguration::class)
class DeadLetterTest {
    @Autowired lateinit var kafka: KafkaTemplate<String, String>
    @Autowired lateinit var producerFactory: ProducerFactory<String, String>
    @Autowired lateinit var consumerFactory: ConsumerFactory<String, String>
    @Autowired lateinit var jdbc: JdbcTemplate
    @Autowired lateinit var tx: TransactionTemplate
    @Autowired lateinit var objectMapper: ObjectMapper
    @Autowired lateinit var kafkaAdmin: KafkaAdmin

    private val maxAttempts = 3
    private val orderId = "order-dlt"
    private lateinit var topic: String
    private lateinit var group: String
    private lateinit var dlt: DeadLetterPublisher
    private var runner: ConsumerRunner? = null
    private val loopsCreated = AtomicInteger()

    @BeforeEach
    fun setUp() {
        jdbc.execute("TRUNCATE orders, point_ledger, processed_messages")
        jdbc.update(
            "INSERT INTO orders (id, status) VALUES (?, 'PENDING')", orderId,
        )
        // 테스트마다 새 토픽(파티션 1개) + DLT. 실패 메시지가 남아도
        // 다른 테스트에 영향이 없다.
        topic = "payment.events.dlt-test-" + UUID.randomUUID()
        group = "point-service-" + UUID.randomUUID()
        kafkaAdmin.createOrModifyTopics(
            TopicBuilder.name(topic).partitions(1).replicas(1).build(),
            TopicBuilder.name(DeadLetterPublisher.dltTopic(topic))
                .partitions(1).replicas(1).build(),
        )
        dlt = DeadLetterPublisher(
            KafkaProducer(producerFactory.configurationProperties),
            maxAttempts, group,
        )
    }

    @AfterEach
    fun tearDown() {
        runner?.stop()
        dlt.close()
    }

    @Test
    fun dead_letter__always_failing_message__sent_to_dlt_after_max_attempts() {
        publish("msg-x") // offset 0: 항상 실패 (poison)
        publish("msg-y") // offset 1: 정상

        start(FailFirstN("msg-x", Int.MAX_VALUE))
        // processedIds()만 기다리면 안 된다: DB 커밋 뒤 같은 pollOnce 안에서
        // commitSync가 나중에 돈다 → 커밋된 offset까지 기다린다
        await { committedOffset() == 2L }

        assertEquals(listOf("msg-y"), processedIds())
        assertEquals(maxAttempts, loopsCreated.get(), "x를 3번 시도")
        val dl = dltRecords(1).single()
        assertEquals(orderId, dl.key(), "원래 key 유지")
        assertEquals("msg-x", dl.header("message_id"), "원래 헤더 유지")
        assertEquals(topic, dl.header(DeadLetterPublisher.ORIGINAL_TOPIC))
        assertEquals("0", dl.header(DeadLetterPublisher.ORIGINAL_PARTITION))
        assertEquals("0", dl.header(DeadLetterPublisher.ORIGINAL_OFFSET))
        assertEquals(
            group, dl.header(DeadLetterPublisher.ORIGINAL_CONSUMER_GROUP),
        )
        assertEquals("3", dl.header(DeadLetterPublisher.ATTEMPTS))
        assertEquals(
            InjectedFailure::class.java.name,
            dl.header(DeadLetterPublisher.EXCEPTION_FQCN),
        )
        assertEquals(
            "handler failure (injected)",
            dl.header(DeadLetterPublisher.EXCEPTION_MESSAGE),
        )
    }

    @Test
    fun dead_letter__missing_message_id__sent_to_dlt_without_retry() {
        publish(null) // offset 0: message_id 헤더 없음
        publish("msg-y")

        start(FailFirstN("never", 0))
        await { committedOffset() == 2L }

        assertEquals(listOf("msg-y"), processedIds())
        assertEquals(1, loopsCreated.get(), "재시도(컨슈머 재생성) 없음")
        val dl = dltRecords(1).single()
        assertEquals("1", dl.header(DeadLetterPublisher.ATTEMPTS))
        assertEquals(
            MissingMessageIdException::class.java.name,
            dl.header(DeadLetterPublisher.EXCEPTION_FQCN),
        )
    }

    @Test
    fun dead_letter__transient_failure__processed_without_dlt() {
        publish("msg-x") // 한 번 실패 후 성공
        publish("msg-y")

        val handler = FailFirstN("msg-x", 1)
        start(handler)
        await { committedOffset() == 2L }

        assertEquals(listOf("msg-x", "msg-y"), processedIds())
        assertEquals(2, handler.calls.get(), "x는 2번 시도")
        assertEquals(0L, dltSize(), "DLT는 비어 있다")
    }

    @Test
    fun dead_letter__db_connection_error__retried_past_max_attempts_without_dlt() {
        publish("msg-x") // DB가 5번 연속 죽어 있다가 살아난다
        publish("msg-y")

        val outages = 5 // > maxAttempts(3)
        val handler = DbDownFirstN("msg-x", outages)
        start(handler)
        // x가 처리되든 DLT로 가든 offset은 2까지 간다
        await(60) { committedOffset() == 2L }
        handler.errors.forEach { println("주입된 DB 장애: $it / cause=${it.cause}") }

        assertEquals(0L, dltSize(), "DB 장애는 DLT로 보내면 안 된다")
        assertEquals(listOf("msg-x", "msg-y"), processedIds())
        assertEquals(outages + 1, handler.calls.get(), "장애 5번 + 성공 1번")
        assertEquals(outages + 1, loopsCreated.get(), "실패마다 컨슈머 재생성")
    }

    @Test
    fun dead_letter__malformed_json__sent_to_dlt_after_max_attempts() {
        publishRaw("msg-bad", "{not json") // 몇 번을 읽어도 깨져 있다
        publish("msg-y")

        start(FailFirstN("never", 0))
        await { committedOffset() == 2L }

        assertEquals(listOf("msg-y"), processedIds())
        assertEquals(maxAttempts, loopsCreated.get(), "깨진 JSON을 3번 시도")
        val dl = dltRecords(1).single()
        assertEquals("msg-bad", dl.header("message_id"))
        assertEquals("{not json", dl.value(), "원래 value 유지")
        assertEquals("3", dl.header(DeadLetterPublisher.ATTEMPTS))
        assertEquals(
            "com.fasterxml.jackson.core.JsonParseException",
            dl.header(DeadLetterPublisher.EXCEPTION_FQCN),
        )
    }

    @Test
    fun dead_letter__dlt_publish_fails__offset_not_committed_past_message() {
        publish("msg-x")
        publish("msg-y")
        dlt.close() // 브로커 장애 대신: 닫힌 프로듀서는 send가 실패한다

        start(FailFirstN("msg-x", Int.MAX_VALUE))
        // 시도 횟수가 maxAttempts를 넘었다 = 한도에 닿아 DLT로 보내려다
        // 실패했고, 커밋 없이 다시 받아 또 실패하는 중이다
        await { dlt.attempts(topic, 0, 0) > maxAttempts }
        runner?.stop()

        val committed = committedOffset()
        assertTrue(
            committed == null || committed <= 0L,
            "x(offset 0)를 넘어 커밋되면 유실: $committed",
        )
        assertEquals(emptyList<String>(), processedIds(), "y도 처리 안 됨")
        assertEquals(0L, dltSize(), "DLT에도 없다")
    }

    // ---- helpers ----

    /** messageId == failId 인 메시지를 처음 n번 실패시킨다 */
    private inner class FailFirstN(
        private val failId: String,
        private val n: Int,
    ) : MessageHandler {
        val calls = AtomicInteger()
        private val inner = IdempotentConsumer(jdbc, tx, group)

        override fun handle(
            messageId: String,
            event: PaymentCompletedEvent,
        ): Boolean {
            if (messageId == failId && calls.incrementAndGet() <= n) {
                throw InjectedFailure()
            }
            return inner.handle(messageId, event)
        }
    }

    /**
     * messageId == failId 인 메시지를 처음 n번은 "죽은 DB"로 처리한다.
     * 127.0.0.1:1 에는 아무도 없으므로 TransactionTemplate이 커넥션을 못 얻어
     * 진짜 CannotCreateTransactionException(← PSQLException 08001)을 던진다.
     * Postgres가 잠깐 내려갔을 때와 같은 예외 체인이다.
     */
    private inner class DbDownFirstN(
        private val failId: String,
        private val n: Int,
    ) : MessageHandler {
        val calls = AtomicInteger()
        val errors: MutableList<Exception> = CopyOnWriteArrayList()
        private val up = IdempotentConsumer(jdbc, tx, group)
        private val down = DriverManagerDataSource(
            "jdbc:postgresql://127.0.0.1:1/lab?connectTimeout=1", "lab", "lab",
        ).let {
            IdempotentConsumer(
                JdbcTemplate(it),
                TransactionTemplate(DataSourceTransactionManager(it)),
                group,
            )
        }

        override fun handle(
            messageId: String,
            event: PaymentCompletedEvent,
        ): Boolean {
            if (messageId == failId && calls.incrementAndGet() <= n) {
                try {
                    return down.handle(messageId, event)
                } catch (e: Exception) {
                    errors += e
                    throw e
                }
            }
            return up.handle(messageId, event)
        }
    }

    private fun start(handler: MessageHandler) {
        runner = ConsumerRunner(retryDelay = Duration.ofMillis(100)) {
            loopsCreated.incrementAndGet()
            ConsumerLoop(
                consumerFactory.createConsumer(group, null),
                topic, handler, objectMapper, dlt,
            )
        }.apply { start() }
    }

    private fun publish(messageId: String?) {
        val event = PaymentCompletedEvent(
            "pay-" + UUID.randomUUID(), orderId, 10_000, "pg_tx",
            Instant.now(),
        )
        val record = ProducerRecord(
            topic, orderId, objectMapper.writeValueAsString(event),
        )
        messageId?.let {
            record.headers().add("message_id", it.toByteArray())
        }
        kafka.send(record).get(10, TimeUnit.SECONDS)
    }

    /** value를 그대로(JSON 검사 없이) 보낸다. poison payload 테스트용 */
    private fun publishRaw(messageId: String, value: String) {
        val record = ProducerRecord(topic, orderId, value)
        record.headers().add("message_id", messageId.toByteArray())
        kafka.send(record).get(10, TimeUnit.SECONDS)
    }

    private fun await(seconds: Long = 30, condition: () -> Boolean) {
        val deadline = Instant.now().plusSeconds(seconds)
        while (!condition()) {
            check(Instant.now() < deadline) { "${seconds}초 안에 조건 미충족" }
            Thread.sleep(100)
        }
    }

    private fun processedIds(): List<String> = jdbc.queryForList(
        "SELECT message_id FROM processed_messages ORDER BY 1",
        String::class.java,
    )

    private fun committedOffset(): Long? {
        val tp = TopicPartition(topic, 0)
        return consumerFactory.createConsumer(group, null)
            .use { it.committed(setOf(tp))[tp]?.offset() }
    }

    private fun dltRecords(want: Int): List<ConsumerRecord<String, String>> {
        val tp = TopicPartition(DeadLetterPublisher.dltTopic(topic), 0)
        val reader = "dlt-reader-" + UUID.randomUUID()
        return consumerFactory.createConsumer(reader, null).use { c ->
            c.assign(listOf(tp))
            c.seekToBeginning(listOf(tp))
            val out = mutableListOf<ConsumerRecord<String, String>>()
            val deadline = Instant.now().plusSeconds(10)
            while (out.size < want && Instant.now() < deadline) {
                out += c.poll(Duration.ofMillis(200))
            }
            out
        }
    }

    private fun dltSize(): Long {
        val tp = TopicPartition(DeadLetterPublisher.dltTopic(topic), 0)
        return consumerFactory.createConsumer("dlt-size", null)
            .use { it.endOffsets(listOf(tp))[tp]!! }
    }

    private fun ConsumerRecord<String, String>.header(name: String) =
        headers().lastHeader(name)?.value()?.let { String(it) }
}
