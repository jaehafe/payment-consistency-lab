package lab.outbox.polling

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

/** relayOnce()를 주기적으로 실행한다. 앱 인스턴스가 여러 대여도 SKIP LOCKED 덕분에 안전하다. */
@Component
@ConditionalOnProperty("lab.outbox.relay.enabled", havingValue = "true")
class OutboxRelayScheduler(
    private val relay: OutboxRelay,
    @Value("\${lab.outbox.relay.batch-size}") private val batchSize: Int,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @Scheduled(fixedDelayString = "\${lab.outbox.relay.interval-ms}")
    fun tick() {
        try {
            // 꽉 찬 배치가 나오면 밀린 게 더 있다는 뜻이므로 바로 한 번 더 돈다.
            while (relay.relayOnce(batchSize) == batchSize) Unit
        } catch (e: Exception) {
            // 브로커 장애 등: outbox에 그대로 남아 있으므로 다음 주기에 재시도된다. 이벤트는 유실되지 않는다.
            log.warn("outbox relay 실패, 다음 주기에 재시도: {}", e.message)
        }
    }
}
