package lab.consumer

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.core.JsonParseException
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.MockConsumer
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.dao.QueryTimeoutException
import org.springframework.jdbc.CannotGetJdbcConnectionException
import org.springframework.transaction.CannotCreateTransactionException
import java.net.ConnectException
import java.sql.SQLTransientConnectionException
import java.time.Duration

/** 브로커·DB 없이 도는 단위 테스트: 분류, 백오프, 설정 검증, 리밸런스 정리 */
class DeadLetterUnitTest {
    private fun producer() =
        MockProducer(true, StringSerializer(), StringSerializer())

    private fun record(partition: Int, offset: Long) =
        ConsumerRecord<String, String>("t", partition, offset, "k", "v")

    @Test
    fun classify__infrastructure_errors__transient() {
        // pgjdbc의 PSQLException은 SQLException을 바로 상속하고 SQLState만 다르다
        // (postgresql 드라이버는 runtimeOnly라 여기서는 SQLException으로 같은 모양을 만든다)
        val connRefused = java.sql.SQLException("Connection refused", "08001")
        listOf(
            // TransactionTemplate + DB 다운에서 실제로 보이는 체인
            CannotCreateTransactionException("Could not open JDBC Connection", connRefused),
            CannotGetJdbcConnectionException("no conn", connRefused),
            QueryTimeoutException("timeout"),
            SQLTransientConnectionException("hikari pool timeout"),
            java.sql.SQLException("terminating connection", "08006"),
            java.sql.SQLException("admin shutdown", "57P01"),
            RuntimeException("wrapped", ConnectException("refused")),
        ).forEach {
            assertEquals(FailureKind.TRANSIENT, FailureClassifier.classify(it), it.toString())
        }
    }

    @Test
    fun classify__deterministic_and_unknown_errors__retryable() {
        listOf(
            JsonParseException(null, "Unrecognized token 'not'"),
            DataIntegrityViolationException("duplicate key"),
            java.sql.SQLException("unique violation", "23505"),
            IllegalArgumentException("amount < 0"),
            InjectedFailure(),
        ).forEach {
            assertEquals(FailureKind.RETRYABLE, FailureClassifier.classify(it), it.toString())
        }
    }

    @Test
    fun classify__missing_message_id__not_retryable_even_when_wrapped() {
        val e = RuntimeException("wrapped", MissingMessageIdException("t-0@1"))
        assertEquals(FailureKind.NOT_RETRYABLE, FailureClassifier.classify(e))
    }

    @Test
    fun record_failure__transient__never_gives_up_or_counts() {
        DeadLetterPublisher(producer(), 3, "g").use { dlt ->
            val rec = record(0, 0)
            val down = CannotCreateTransactionException("db down")
            repeat(10) { assertFalse(dlt.recordFailure(rec, down)) }
            assertEquals(0, dlt.attempts("t", 0, 0))
            // retryable은 센다 → 3번째에 포기
            assertFalse(dlt.recordFailure(rec, InjectedFailure()))
            assertFalse(dlt.recordFailure(rec, InjectedFailure()))
            assertTrue(dlt.recordFailure(rec, InjectedFailure()))
        }
    }

    @Test
    fun backoff__doubles_up_to_cap() {
        val base = Duration.ofSeconds(1)
        val max = Duration.ofSeconds(30)
        mapOf(1 to 1L, 2 to 2L, 3 to 4L, 5 to 16L, 6 to 30L, 7 to 30L, 1000 to 30L)
            .forEach { (failures, want) ->
                assertEquals(
                    Duration.ofSeconds(want),
                    ConsumerRunner.backoff(base, max, failures), "failures=$failures",
                )
            }
    }

    @Test
    fun config__invalid_values__rejected_at_construction() {
        assertThrows<IllegalArgumentException> { DeadLetterPublisher(producer(), 0, "g") }
        assertThrows<IllegalArgumentException> { DeadLetterPublisher(producer(), -1, "g") }
        assertThrows<IllegalArgumentException> {
            DeadLetterPublisher(producer(), 3, "g", sendTimeout = Duration.ZERO)
        }
        assertThrows<IllegalArgumentException> {
            ConsumerRunner(retryDelay = Duration.ZERO) { error("unused") }
        }
        assertThrows<IllegalArgumentException> {
            ConsumerRunner(Duration.ofSeconds(5), Duration.ofSeconds(1)) { error("unused") }
        }
        DeadLetterPublisher(producer(), 1, "g").close() // 1은 유효
    }

    @Test
    fun rebalance__revoked_partitions_forgotten_but_not_on_close() {
        val dlt = DeadLetterPublisher(producer(), 3, "g")
        val consumer = MockConsumer<String, String>(OffsetResetStrategy.EARLIEST)
        val loop = ConsumerLoop(consumer, "t", { _, _ -> true }, ObjectMapper(), dlt)
        val tp0 = TopicPartition("t", 0)
        val tp1 = TopicPartition("t", 1)
        consumer.rebalance(listOf(tp0, tp1))
        dlt.recordFailure(record(0, 5), InjectedFailure())
        dlt.recordFailure(record(1, 5), InjectedFailure())

        // 리밸런스로 파티션 0이 다른 인스턴스로 넘어감
        loop.rebalanceListener.onPartitionsRevoked(listOf(tp0))
        assertEquals(0, dlt.attempts("t", 0, 5), "빠진 파티션은 지운다")
        assertEquals(1, dlt.attempts("t", 1, 5), "남은 파티션은 유지")

        loop.rebalanceListener.onPartitionsLost(listOf(tp1))
        assertEquals(0, dlt.attempts("t", 1, 5), "lost도 지운다")

        // close() 중의 revoke(ConsumerRunner의 재생성)는 무시 → 재시도 횟수가 이어진다
        dlt.recordFailure(record(1, 6), InjectedFailure())
        loop.close()
        loop.rebalanceListener.onPartitionsRevoked(listOf(tp1))
        assertEquals(1, dlt.attempts("t", 1, 6), "close 중 revoke는 무시")
        dlt.close()
    }

    @Test
    fun rebalance__assigned__keeps_only_current_assignment() {
        val dlt = DeadLetterPublisher(producer(), 3, "g")
        val consumer = MockConsumer<String, String>(OffsetResetStrategy.EARLIEST)
        ConsumerLoop(consumer, "t", { _, _ -> true }, ObjectMapper(), dlt).use {
            dlt.recordFailure(record(0, 5), InjectedFailure())
            dlt.recordFailure(record(1, 5), InjectedFailure())
            // 재생성한 컨슈머가 파티션 1만 받음 (0은 그 사이 다른 인스턴스로)
            consumer.rebalance(listOf(TopicPartition("t", 1)))
            assertEquals(0, dlt.attempts("t", 0, 5))
            assertEquals(1, dlt.attempts("t", 1, 5))
        }
        dlt.close()
    }
}
