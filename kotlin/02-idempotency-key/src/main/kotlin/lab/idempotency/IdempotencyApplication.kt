package lab.idempotency

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class IdempotencyApplication

fun main(args: Array<String>) {
    runApplication<IdempotencyApplication>(*args)
}
