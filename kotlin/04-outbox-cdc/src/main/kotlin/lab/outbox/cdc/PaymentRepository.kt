package lab.outbox.cdc

import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Repository

@Repository
class PaymentRepository(private val jdbc: JdbcTemplate) {
    fun insert(p: Payment) {
        jdbc.update(
            "INSERT INTO payments (id, order_id, amount, status, pg_tx_id) VALUES (?, ?, ?, ?, ?)",
            p.id, p.orderId, p.amount, p.status, p.pgTxId,
        )
    }

    fun countByOrderId(orderId: String): Int =
        jdbc.queryForObject("SELECT count(*) FROM payments WHERE order_id = ?", Int::class.java, orderId)!!
}
