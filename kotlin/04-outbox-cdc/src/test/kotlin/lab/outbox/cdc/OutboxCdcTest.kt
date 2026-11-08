package lab.outbox.cdc

import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.JdbcTemplate
import java.util.UUID

@SpringBootTest
@Import(TestcontainersConfiguration::class)
class OutboxCdcTest {
    @Autowired lateinit var service: PaymentService
    @Autowired lateinit var jdbc: JdbcTemplate
    @Autowired lateinit var objectMapper: ObjectMapper

    @Test
    fun pay__writes_outbox_row() {
        val orderId = "order-" + UUID.randomUUID()

        val payment = service.pay(orderId, 10_000)

        val row = jdbc.queryForMap(
            "SELECT aggregatetype, aggregateid, type, payload::text AS payload FROM outbox WHERE aggregateid = ?",
            orderId,
        )
        // 이 값들이 Debezium Outbox Event Router 설정(infra/debezium/outbox-connector.json)과 맞물린다
        assertEquals("payment", row["aggregatetype"])
        assertEquals(orderId, row["aggregateid"])
        assertEquals("PaymentCompleted", row["type"])
        val payload = objectMapper.readTree(row["payload"] as String)
        assertEquals(payment.id.toString(), payload["paymentId"].asText())
        assertEquals(orderId, payload["orderId"].asText())
        assertEquals(10_000, payload["amount"].asLong())
        assertEquals(payment.pgTxId, payload["pgTxId"].asText())

        val paymentCount = jdbc.queryForObject("SELECT count(*) FROM payments WHERE order_id = ?", Int::class.java, orderId)
        assertEquals(1, paymentCount)
    }
}
