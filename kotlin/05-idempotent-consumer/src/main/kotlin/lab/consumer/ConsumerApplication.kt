package lab.consumer

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.admin.NewTopic
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.transaction.support.TransactionTemplate

@SpringBootApplication
class ConsumerApplication {
    @Bean
    fun paymentEventsTopic(@Value("\${lab.topic}") topic: String): NewTopic =
        TopicBuilder.name(topic).partitions(3).replicas(1).build()

    /** 앱으로 실행할 때는 멱등 컨슈머를 백그라운드 스레드에서 계속 돌린다. */
    @Bean
    @ConditionalOnProperty("lab.consumer.enabled", havingValue = "true")
    fun consumerRunner(
        consumerFactory: ConsumerFactory<String, String>,
        jdbc: JdbcTemplate,
        tx: TransactionTemplate,
        objectMapper: ObjectMapper,
        @Value("\${lab.topic}") topic: String,
        @Value("\${lab.consumer.group-id}") groupId: String,
    ) = ConsumerRunner {
        ConsumerLoop(consumerFactory.createConsumer(groupId, null), topic, IdempotentConsumer(jdbc, tx, groupId), objectMapper)
    }
}

fun main(args: Array<String>) {
    runApplication<ConsumerApplication>(*args)
}
