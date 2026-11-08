package lab.consumer

/** 메시지 한 건을 처리한다. 리턴 전에 DB 트랜잭션 커밋까지 끝나 있어야 한다. */
fun interface MessageHandler {
    /** @return true = 처리함, false = 이미 처리한 메시지라 건너뜀 */
    fun handle(messageId: String, event: PaymentCompletedEvent): Boolean
}

/** 결제 완료 시 실행되는 비즈니스 로직 (두 컨슈머가 공유) */
internal object PaymentCompletedEffects {
    fun apply(jdbc: org.springframework.jdbc.core.JdbcTemplate, event: PaymentCompletedEvent) {
        // 주문 상태 갱신: "PAID로, 금액은 이것으로" 덮어쓰기라 몇 번 실행해도 결과가 같다 (자연적으로 멱등).
        jdbc.update("UPDATE orders SET status = 'PAID', paid_amount = ? WHERE id = ?", event.amount, event.orderId)
        // 포인트 적립: 실행할 때마다 행이 하나씩 쌓인다 (멱등하지 않음). 중복 메시지가 오면 포인트가 두 번 지급된다.
        jdbc.update("INSERT INTO point_ledger (order_id, points) VALUES (?, ?)", event.orderId, event.amount / 100)
    }
}
