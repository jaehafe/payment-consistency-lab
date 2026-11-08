package lab.outbox.polling

import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.dao.DataAccessException
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.kafka.KafkaException
import org.springframework.transaction.support.TransactionTemplate
import org.testcontainers.redpanda.RedpandaContainer
import java.io.File
import java.time.Duration
import java.util.UUID
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

// 스케줄러를 꺼서, 테스트가 relayOnce()를 원하는 시점에 직접 호출하도록 한다.
@SpringBootTest(properties = ["lab.outbox.relay.enabled=false"])
@Import(TestcontainersConfiguration::class)
class OutboxPollingTest {
    @Autowired lateinit var service: PaymentService
    @Autowired lateinit var relay: OutboxRelay
    @Autowired lateinit var outbox: OutboxRepository
    @Autowired lateinit var publisher: OutboxPublisher
    @Autowired lateinit var dbFailure: DbFailureInjector
    @Autowired lateinit var payments: PaymentRepository
    @Autowired lateinit var tx: TransactionTemplate
    @Autowired lateinit var jdbc: JdbcTemplate
    @Autowired lateinit var objectMapper: ObjectMapper
    @Autowired lateinit var redpanda: RedpandaContainer

    private val probe by lazy { EventProbe(redpanda.bootstrapServers, "payment.events") }

    @BeforeEach
    fun cleanTables() {
        jdbc.execute("TRUNCATE outbox_polling, payments")
    }

    @AfterEach
    fun resetToggles() {
        publisher.brokerDown = false
        publisher.latency = Duration.ZERO
        relay.crashAfterPublish = false
        dbFailure.failBeforeCommit = false
    }

    @Test
    fun pay__writes_payment_and_outbox_atomically() {
        val orderId = newOrderId()

        val payment = service.pay(orderId, 10_000)

        assertEquals(1, payments.countByOrderId(orderId))
        val row = jdbc.queryForMap(
            "SELECT aggregate_type, event_type, payload::text AS payload, published_at FROM outbox_polling WHERE aggregate_id = ?",
            orderId,
        )
        assertEquals("payment", row["aggregate_type"])
        assertEquals("PaymentCompleted", row["event_type"])
        assertEquals(payment.id.toString(), objectMapper.readTree(row["payload"] as String)["paymentId"].asText())
        assertEquals(null, row["published_at"], "아직 relay가 돌지 않았으므로 미발행")
        assertEquals(0, probe.eventsFor(orderId).size, "pay()는 Kafka를 직접 호출하지 않는다")
    }

    @Test
    fun pay__db_failure__no_payment_no_outbox() {
        val orderId = newOrderId()
        dbFailure.failBeforeCommit = true

        assertThrows<DataAccessException> { service.pay(orderId, 10_000) }

        // 같은 트랜잭션이므로 함께 롤백된다 — 01의 "유령 이벤트"가 원천적으로 불가능
        assertEquals(0, payments.countByOrderId(orderId))
        assertEquals(0, countOutbox(orderId))
    }

    @Test
    fun broker_down__payment_saved__event_delivered_after_recovery() {
        val orderId = newOrderId()
        publisher.brokerDown = true

        service.pay(orderId, 10_000) // 브로커가 죽어 있어도 결제 요청 자체는 성공한다

        assertThrows<KafkaException> { relay.relayOnce(10) }
        assertEquals(1, payments.countByOrderId(orderId))
        assertEquals(1, outbox.countUnpublished(), "발행 실패 → 롤백 → outbox에 그대로 남아 있다 (유실 없음)")
        assertEquals(0, probe.eventsFor(orderId).size)

        publisher.brokerDown = false // 브로커 복구
        assertEquals(1, relay.relayOnce(10))

        assertEquals(0, outbox.countUnpublished())
        val events = probe.eventsFor(orderId)
        assertEquals(1, events.size, "복구 후 relay가 밀린 이벤트를 전달했다 — 01의 '이벤트 유실'이 해결됨")
        assertEquals(outboxId(orderId), events.single().messageId)
        assertEquals("PaymentCompleted", events.single().eventType, "event_type 헤더 (04의 Debezium도 같은 헤더를 붙인다)")
    }

    @Test
    fun two_relays_concurrently__each_event_published_once() {
        val orderIds = (1..40).map { newOrderId() }
        orderIds.forEach { service.pay(it, 10_000) }
        publisher.latency = Duration.ofMillis(10) // 발행이 느려서 두 relay의 실행 구간이 확실히 겹치게

        // 같은 DB를 바라보는 relay 두 대 (= 앱 인스턴스 두 대)
        val relayA = OutboxRelay(tx, outbox, publisher)
        val relayB = OutboxRelay(tx, outbox, publisher)
        val pool = Executors.newFixedThreadPool(2)
        fun drain(r: OutboxRelay) = Callable {
            var published = 0
            // 상대가 잠근 행은 SKIP되어 0이 나올 수 있으므로, 미발행 행이 없어질 때까지 돈다.
            while (outbox.countUnpublished() > 0) published += r.relayOnce(5)
            published
        }
        val a = pool.submit(drain(relayA))
        val b = pool.submit(drain(relayB))
        val publishedByA = a.get(60, TimeUnit.SECONDS)
        val publishedByB = b.get(60, TimeUnit.SECONDS)
        pool.shutdown()

        println("relayA=$publishedByA, relayB=$publishedByB")
        assertTrue(publishedByA > 0 && publishedByB > 0, "두 relay가 실제로 일을 나눠 가졌다: A=$publishedByA B=$publishedByB")
        assertEquals(40, publishedByA + publishedByB)

        val keys = orderIds.toSet()
        val events = probe.events { it.key in keys }
        assertEquals(40, events.size, "중복 없이 정확히 40건")
        assertEquals(40, events.map { it.messageId }.toSet().size, "message_id가 모두 다르다 = 같은 행을 두 relay가 발행하지 않았다")
    }

    @Test
    fun crash_after_publish_before_mark__event_published_twice() {
        val orderId = newOrderId()
        service.pay(orderId, 10_000)

        relay.crashAfterPublish = true
        assertThrows<SimulatedCrashException> { relay.relayOnce(10) } // 발행은 됐지만 published_at 커밋 전에 죽음
        assertEquals(1, outbox.countUnpublished(), "published_at이 롤백되어 여전히 미발행 상태")

        relay.crashAfterPublish = false // 재기동한 relay
        relay.relayOnce(10)

        val events = probe.eventsFor(orderId)
        assertEquals(2, events.size, "같은 이벤트가 두 번 발행됐다 (at-least-once)")
        assertEquals(1, events.map { it.messageId }.toSet().size, "둘 다 같은 message_id → 컨슈머가 이것으로 중복을 걸러낸다 (05)")
    }

    /**
     * 03과 04는 로컬에서 같은 Postgres(infra/docker-compose.yml의 lab DB)를 쓴다. 03은 스키마 k03,
     * 04는 Debezium 커넥터(table.include.list = public.outbox) 때문에 public 에 테이블을 둔다.
     * 03이 public 에 같은 이름(payments/outbox)으로 다른 컬럼의 테이블을 먼저 만들어 두면, 04의 `CREATE TABLE IF NOT EXISTS`는
     * 아무 일도 안 하고 넘어가고 04의 insert가 "column aggregatetype does not exist"로 깨진다.
     * → 03을 돌린 DB에서 04의 커넥션처럼 search_path=public 으로 04 스키마를 적용하고 04 방식의 insert가 되는지 확인한다.
     *   (DDL도 롤백되는 Postgres 트랜잭션 안에서. SET LOCAL 이라 커밋/롤백과 함께 원래 search_path(k03)로 돌아온다)
     */
    @Test
    fun schema__does_not_collide_with_04_cdc_outbox_table() {
        val cdcSchema = File("../04-outbox-cdc/src/main/resources/schema.sql").readText()
        tx.executeWithoutResult { status ->
            jdbc.execute("SET LOCAL search_path TO public") // 04의 커넥션 = public
            jdbc.execute(cdcSchema)
            jdbc.update(
                "INSERT INTO outbox (id, aggregatetype, aggregateid, type, payload) VALUES (?, 'payment', 'o', 'PaymentCompleted', '{}'::jsonb)",
                UUID.randomUUID(),
            )
            status.setRollbackOnly() // 04의 테이블은 이 테스트 DB에 남기지 않는다
        }
    }

    private fun countOutbox(orderId: String) =
        jdbc.queryForObject("SELECT count(*) FROM outbox_polling WHERE aggregate_id = ?", Int::class.java, orderId)!!

    private fun outboxId(orderId: String) =
        jdbc.queryForObject("SELECT id::text FROM outbox_polling WHERE aggregate_id = ?", String::class.java, orderId)

    private fun newOrderId() = "order-" + UUID.randomUUID()
}
