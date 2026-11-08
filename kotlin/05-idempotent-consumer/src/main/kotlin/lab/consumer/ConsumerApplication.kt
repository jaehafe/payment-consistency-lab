package lab.consumer

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.ProducerFactory
import org.springframework.transaction.support.TransactionTemplate
import java.time.Duration

@SpringBootApplication
class ConsumerApplication {
    @Bean
    fun paymentEventsTopic(@Value("\${lab.topic}") topic: String): NewTopic =
        TopicBuilder.name(topic).partitions(3).replicas(1).build()

    /** DLT도 미리 만든다. 이름은 Spring Kafka 3.3+ 기본값인 "<topic>-dlt" */
    @Bean
    fun paymentEventsDlt(@Value("\${lab.topic}") topic: String): NewTopic =
        TopicBuilder.name(DeadLetterPublisher.dltTopic(topic))
            .partitions(3).replicas(1).build()

    /**
     * 실패 횟수를 들고 있으므로 ConsumerRunner가 루프를 새로
     * 만들어도 살아 있어야 한다 → 빈 하나.
     * 프로듀서는 KafkaTemplate이 아니라 kafka-clients의
     * KafkaProducer 그대로 (설정만 spring.kafka.* 에서 가져온다)
     */
    @Bean
    @ConditionalOnProperty("lab.consumer.enabled", havingValue = "true")
    fun deadLetterPublisher(
        producerFactory: ProducerFactory<String, String>,
        @Value("\${lab.consumer.group-id}") groupId: String,
        @Value("\${lab.consumer.dlt.max-attempts}") maxAttempts: Int,
    ) = DeadLetterPublisher(
        KafkaProducer(producerFactory.configurationProperties),
        maxAttempts, groupId,
    )

    /** 앱으로 실행할 때는 멱등 컨슈머를 백그라운드 스레드에서 계속 돌린다. */
    @Bean
    @ConditionalOnProperty("lab.consumer.enabled", havingValue = "true")
    fun consumerRunner(
        consumerFactory: ConsumerFactory<String, String>,
        jdbc: JdbcTemplate,
        tx: TransactionTemplate,
        objectMapper: ObjectMapper,
        deadLetter: DeadLetterPublisher,
        @Value("\${lab.topic}") topic: String,
        @Value("\${lab.consumer.group-id}") groupId: String,
        @Value("\${lab.consumer.retry.initial-delay}") retryDelay: Duration,
        @Value("\${lab.consumer.retry.max-delay}") maxRetryDelay: Duration,
    ) = ConsumerRunner(retryDelay, maxRetryDelay) {
        ConsumerLoop(
            consumerFactory.createConsumer(groupId, null), topic,
            IdempotentConsumer(jdbc, tx, groupId), objectMapper, deadLetter,
        )
    }
}

fun main(args: Array<String>) {
    runApplication<ConsumerApplication>(*args)
}
