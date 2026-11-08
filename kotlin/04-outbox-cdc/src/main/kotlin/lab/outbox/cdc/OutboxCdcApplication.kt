package lab.outbox.cdc

import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Profile
import java.util.UUID

@SpringBootApplication
class OutboxCdcApplication {
    /**
     * 수동 실습용: `--spring.profiles.active=demo`로 실행하면 결제 한 건을 만들고 종료한다.
     * Debezium 커넥터가 등록돼 있으면 곧 payment.events 토픽에 메시지가 나타난다 (README 참고).
     */
    @Bean
    @Profile("demo")
    fun demo(service: PaymentService) = CommandLineRunner {
        val payment = service.pay("order-" + UUID.randomUUID(), 10_000)
        println("결제 저장 + outbox insert 완료: $payment")
    }
}

fun main(args: Array<String>) {
    runApplication<OutboxCdcApplication>(*args)
}
