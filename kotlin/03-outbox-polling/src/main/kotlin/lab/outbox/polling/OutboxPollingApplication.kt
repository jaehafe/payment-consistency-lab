package lab.outbox.polling

import org.apache.kafka.clients.admin.NewTopic
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.kafka.config.TopicBuilder
import org.springframework.scheduling.annotation.EnableScheduling

@SpringBootApplication
@EnableScheduling
class OutboxPollingApplication {
    @Bean
    fun paymentEventsTopic(@Value("\${lab.topic}") topic: String): NewTopic =
        TopicBuilder.name(topic).partitions(3).replicas(1).build()
}

fun main(args: Array<String>) {
    runApplication<OutboxPollingApplication>(*args)
}
