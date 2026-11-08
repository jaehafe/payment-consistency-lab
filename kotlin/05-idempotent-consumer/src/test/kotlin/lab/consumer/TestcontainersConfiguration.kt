package lab.consumer

import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.context.annotation.Bean
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.redpanda.RedpandaContainer
import org.testcontainers.utility.DockerImageName

/**
 * 실제 Postgres 16 + Redpanda(Kafka 호환)를 컨테이너로 띄운다.
 * @ServiceConnection 덕분에 spring.datasource.* / spring.kafka.bootstrap-servers가 자동으로 컨테이너를 가리킨다.
 */
@TestConfiguration(proxyBeanMethods = false)
class TestcontainersConfiguration {
    @Bean
    @ServiceConnection
    fun postgresContainer(): PostgreSQLContainer<*> = PostgreSQLContainer(DockerImageName.parse("postgres:16"))

    @Bean
    @ServiceConnection
    fun redpandaContainer(): RedpandaContainer = RedpandaContainer(DockerImageName.parse("redpandadata/redpanda:v24.2.7"))
}
