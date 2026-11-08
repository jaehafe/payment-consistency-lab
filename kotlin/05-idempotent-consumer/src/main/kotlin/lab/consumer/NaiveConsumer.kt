package lab.consumer

import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.transaction.support.TransactionTemplate

/**
 * [안티패턴] 메시지가 올 때마다 무조건 처리한다.
 * 브로커/outbox relay는 at-least-once이므로 같은 메시지가 두 번 오는 건 "버그"가 아니라 "정상 동작"이다.
 * 그런데 이 컨슈머는 그걸 고려하지 않아서 포인트가 두 번 쌓인다.
 */
class NaiveConsumer(
    private val jdbc: JdbcTemplate,
    private val tx: TransactionTemplate,
) : MessageHandler {
    override fun handle(messageId: String, event: PaymentCompletedEvent): Boolean {
        tx.executeWithoutResult { PaymentCompletedEffects.apply(jdbc, event) }
        return true
    }
}
