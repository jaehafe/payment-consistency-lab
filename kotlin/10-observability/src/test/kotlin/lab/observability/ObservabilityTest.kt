package lab.observability

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.micrometer.core.instrument.MeterRegistry
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.system.CapturedOutput
import org.springframework.boot.test.system.OutputCaptureExtension
import org.springframework.boot.test.web.client.TestRestTemplate
import org.springframework.context.annotation.Import
import org.springframework.http.HttpEntity
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import java.util.UUID
import java.util.concurrent.TimeUnit

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TestcontainersConfiguration::class)
// 스프링 부트 테스트는 기본적으로 메트릭 export(Prometheus 레지스트리)를 끈다 → 실제 /actuator/prometheus를 검증하려고 켠다
@AutoConfigureObservability
@ExtendWith(OutputCaptureExtension::class)
class ObservabilityTest {
    @Autowired lateinit var http: TestRestTemplate
    @Autowired lateinit var registry: MeterRegistry
    @Autowired lateinit var pg: FakePaymentGateway
    @Autowired lateinit var objectMapper: ObjectMapper

    @BeforeEach
    fun setUp() = pg.reset()

    @Test
    fun request_id_propagated_to_all_logs(output: CapturedOutput) {
        val requestId = "req-" + UUID.randomUUID()
        val orderId = newOrderId()

        val response = post(newKey(), orderId, 10_000, requestId)

        assertEquals(201, response.statusCode.value())
        assertEquals(requestId, response.headers.getFirst("X-Request-Id"), "응답 헤더로 돌려준다")

        // 콘솔은 한 줄 JSON(logstash 포맷). 이 요청이 남긴 로그를 모은다.
        val logs = jsonLogs(output).filter { it.path("request_id").asText() == requestId }
        logs.forEach { println(it) }
        val messages = logs.map { it.path("message").asText() }
        assertTrue(messages.any { it.startsWith("결제 요청 수신") }, "컨트롤러/서비스 진입 로그: $messages")
        assertTrue(messages.any { it.startsWith("PG 승인 요청") })
        assertTrue(messages.any { it.startsWith("PG 응답") })
        assertTrue(messages.any { it.startsWith("결제 승인") })
        assertTrue(messages.any { it.startsWith("요청 완료") }, "필터의 마지막 로그까지")

        // 반대 방향 검증: 이 주문에 대한 로그 중 request_id가 빠지거나 다른 것이 하나도 없다
        val orderLogs = jsonLogs(output).filter { it.path("order_id").asText() == orderId }
        assertTrue(orderLogs.size >= 4)
        assertTrue(orderLogs.all { it.path("request_id").asText() == requestId }, "주문 로그 전부 같은 request_id")
        // 결제 생성 이후 로그에는 payment_id, idempotency_key도 붙는다
        assertTrue(orderLogs.filter { it.path("message").asText().startsWith("결제 승인") }.all { it.hasNonNull("payment_id") && it.hasNonNull("idempotency_key") })
    }

    @Test
    fun pg_timeout__unknown_counter_and_gauge_increase() {
        val unknownBefore = requestsCount("unknown")
        val gaugeBefore = unknownGauge()
        pg.timeoutAfterCharge = true

        val response = post(newKey(), newOrderId(), 10_000)

        assertEquals(202, response.statusCode.value(), "타임아웃 → 202 Accepted (결과 미정)")
        assertEquals("UNKNOWN", json(response.body)["status"].asText())
        assertEquals(unknownBefore + 1, requestsCount("unknown"), "payment_requests_total{result=\"unknown\"} +1")
        assertEquals(gaugeBefore + 1, unknownGauge(), "payment_unknown_current +1 (DB의 UNKNOWN 건수)")
    }

    @Test
    fun duplicate_requests__replay_metric_increments() {
        val replaysBefore = registry.get("idempotency.replays").counter().count()
        val replayedBefore = requestsCount("replayed")
        val approvedBefore = requestsCount("approved")
        val key = newKey()
        val orderId = newOrderId()

        repeat(3) { post(key, orderId, 10_000) } // 클라이언트 재시도 폭주

        assertEquals(replaysBefore + 2, registry.get("idempotency.replays").counter().count(), "idempotency_replays_total +2")
        assertEquals(replayedBefore + 2, requestsCount("replayed"))
        assertEquals(approvedBefore + 1, requestsCount("approved"), "실제 승인은 1번")
    }

    @Test
    fun pg_latency__histogram_records_duration() {
        val timer = { registry.find("pg.call.duration").tag("outcome", "success").timer() }
        val countBefore = timer()?.count() ?: 0
        val totalBefore = timer()?.totalTime(TimeUnit.MILLISECONDS) ?: 0.0
        pg.latencyMs = 300 // PG 지연 급증

        post(newKey(), newOrderId(), 10_000)

        val t = timer()!!
        assertEquals(countBefore + 1, t.count())
        assertTrue(t.totalTime(TimeUnit.MILLISECONDS) - totalBefore >= 300.0, "300ms 이상 기록됨")
        // histogram 버킷: 0.3초 이하 버킷에는 이번 호출이 들어가지 않는다 (le < 0.3 버킷 개수는 이전과 같아야 함)
        val buckets = t.takeSnapshot().histogramCounts()
        assertTrue(buckets.isNotEmpty(), "publishPercentileHistogram → _bucket 시계열")
        val overallCount = buckets.last().count()
        val under250ms = buckets.filter { it.bucket(TimeUnit.MILLISECONDS) <= 250.0 }.maxOfOrNull { it.count() } ?: 0.0
        assertTrue(overallCount - under250ms >= 1.0, "느린 호출이 큰 버킷(> 250ms)에 들어갔다")
    }

    @Test
    fun metrics_endpoint_exposes_payment_metrics() {
        post(newKey(), newOrderId(), 10_000)

        val body = http.getForObject("/actuator/prometheus", String::class.java)

        // alerts.yml 이 쓰는 이름 그대로 노출되는지
        assertTrue(body.contains("payment_requests_total{"), "payment_requests_total")
        listOf("approved", "failed", "unknown", "replayed", "conflict").forEach {
            assertTrue(body.contains("result=\"$it\""), "result=$it 라벨이 0이라도 미리 노출됨")
        }
        assertTrue(body.contains("pg_call_duration_seconds_bucket{"), "pg_call_duration_seconds histogram")
        assertTrue(body.contains("pg_call_duration_seconds_count{"))
        assertTrue(Regex("(?m)^payment_unknown_current\\{.*} ").containsMatchIn(body), "payment_unknown_current gauge")
        assertTrue(Regex("(?m)^idempotency_replays_total\\{.*} ").containsMatchIn(body), "idempotency_replays_total")
    }

    // ---- helpers ----

    private fun post(key: String, orderId: String, amount: Long, requestId: String? = null): ResponseEntity<String> {
        val headers = HttpHeaders().apply {
            contentType = MediaType.APPLICATION_JSON
            set("Idempotency-Key", key)
            requestId?.let { set("X-Request-Id", it) }
        }
        return http.postForEntity("/payments", HttpEntity("""{"orderId": "$orderId", "amount": $amount}""", headers), String::class.java)
    }

    private fun jsonLogs(output: CapturedOutput): List<JsonNode> =
        output.out.lines().filter { it.startsWith("{") }.mapNotNull { runCatching { objectMapper.readTree(it) }.getOrNull() }

    private fun requestsCount(result: String) = registry.get("payment.requests").tag("result", result).counter().count()
    private fun unknownGauge() = registry.get("payment.unknown.current").gauge().value()
    private fun json(s: String?) = objectMapper.readTree(s)
    private fun newKey() = UUID.randomUUID().toString()
    private fun newOrderId() = "order-" + UUID.randomUUID()
}
