package lab.idempotency

import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Repository
import java.util.UUID

@Repository
class PaymentRepository(private val jdbc: JdbcTemplate) {
    fun insert(id: UUID, orderId: String, amount: Long, pgTxId: String) {
        jdbc.update(
            "INSERT INTO payments (id, order_id, amount, status, pg_tx_id) VALUES (?, ?, ?, 'COMPLETED', ?)",
            id, orderId, amount, pgTxId,
        )
    }

    fun findByOrderId(orderId: String): PaymentResponse =
        jdbc.queryForObject(
            "SELECT id, order_id, amount, pg_tx_id, status FROM payments WHERE order_id = ?",
            { rs, _ ->
                PaymentResponse(
                    paymentId = rs.getString("id"),
                    orderId = rs.getString("order_id"),
                    amount = rs.getLong("amount"),
                    pgTxId = rs.getString("pg_tx_id"),
                    status = rs.getString("status"),
                )
            },
            orderId,
        )!!

    fun countByOrderId(orderId: String): Int =
        jdbc.queryForObject("SELECT count(*) FROM payments WHERE order_id = ?", Int::class.java, orderId)!!
}
