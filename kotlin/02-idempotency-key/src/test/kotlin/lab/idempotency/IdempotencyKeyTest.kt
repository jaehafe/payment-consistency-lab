package lab.idempotency

import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.client.TestRestTemplate
import org.springframework.context.annotation.Import
import org.springframework.http.HttpEntity
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.jdbc.core.JdbcTemplate
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/** 실제 HTTP 서버(랜덤 포트) + 실제 Postgres로 POST /payments 를 호출한다. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TestcontainersConfiguration::class)
class IdempotencyKeyTest {
    @Autowired lateinit var http: TestRestTemplate
    @Autowired lateinit var pg: FakePaymentGateway
    @Autowired lateinit var payments: PaymentRepository
    @Autowired lateinit var jdbc: JdbcTemplate
    @Autowired lateinit var objectMapper: ObjectMapper

    @BeforeEach
    fun setUp() = pg.reset()

    @Test
    fun same_key_twice__charged_once__same_response() {
        val key = newKey()
        val body = body(newOrderId(), 10_000)

        val first = post(key, body)
        val second = post(key, body) // 클라이언트가 응답을 못 받아서 재시도했다고 가정

        assertEquals(201, first.statusCode.value())
        assertEquals(201, second.statusCode.value())
        assertEquals(json(first.body), json(second.body), "두 번째는 저장된 첫 응답을 그대로 재생한다")
        assertEquals(first.body, second.body, "바이트 단위까지 같다 (첫 응답도 jsonb에 저장된 바디를 돌려줌)")
        assertEquals(1, pg.chargeCount(), "실제 승인은 한 번")
        assertEquals(1, pg.callCount(), "두 번째 요청은 PG를 호출조차 하지 않는다 (finished → 저장된 응답)")
    }

    @Test
    fun same_key_different_body__422() {
        val key = newKey()
        val orderId = newOrderId()
        assertEquals(201, post(key, body(orderId, 10_000)).statusCode.value())

        val response = post(key, body(orderId, 99_000)) // 같은 key, 다른 금액

        assertEquals(422, response.statusCode.value())
        assertEquals(1, pg.chargeCount())
    }

    @Test
    fun concurrent_same_key__only_one_charge() {
        val key = newKey()
        val orderId = newOrderId()
        val body = body(orderId, 10_000)
        pg.latency = Duration.ofMillis(500) // 첫 요청이 PG 호출 중일 때 나머지가 몰려오도록

        val threads = 10
        val ready = CountDownLatch(threads)
        val go = CountDownLatch(1)
        val pool = Executors.newFixedThreadPool(threads)
        val futures = (1..threads).map {
            pool.submit<ResponseEntity<String>> {
                ready.countDown()
                go.await() // 모든 스레드가 준비되면 동시에 출발
                post(key, body)
            }
        }
        ready.await()
        go.countDown()
        val responses = futures.map { it.get(30, TimeUnit.SECONDS) }
        pool.shutdown()

        val statuses = responses.map { it.statusCode.value() }
        println("동시 요청 응답 코드: $statuses")
        // 불변식은 "PG 승인 1회"다. 201의 개수는 "처리한 1건 + 처리가 끝난 뒤 도착해 저장된 응답을 재생받은 요청"이라
        // 타이밍에 따라 1보다 클 수 있다 → ==1 로 단언하지 않는다 (보통은 201×1, 409×9).
        assertTrue(statuses.all { it == 201 || it == 409 }, "201(처리/재생) 또는 409(처리 중)만, 5xx 없음: $statuses")
        assertTrue(statuses.count { it == 201 } >= 1, "적어도 1건은 처리되어 201: $statuses")
        assertTrue(statuses.count { it == 409 } >= 1, "경합이 실제로 일어나 409가 있어야 한다: $statuses")
        val createdBodies = responses.filter { it.statusCode.value() == 201 }.map { json(it.body) }.distinct()
        assertEquals(1, createdBodies.size, "201 응답들은 모두 같은 결제(같은 바디)여야 한다: $createdBodies")
        assertEquals(1, pg.callCount(), "Phase 1 잠금 덕분에 PG 호출 자체가 한 번뿐")
        assertEquals(1, pg.chargeCount())
        assertEquals(1, payments.countByOrderId(orderId))
    }

    @Test
    fun pg_timeout_after_charge__retry_resumes__no_double_charge() {
        val key = newKey()
        val orderId = newOrderId()
        val body = body(orderId, 10_000)

        // 1차 시도: PG는 승인했는데 응답이 유실됐다 → 우리는 실패(503)로 응답
        pg.timeoutAfterCharge = true
        assertEquals(503, post(key, body).statusCode.value())
        assertEquals(1, pg.chargeCount(), "돈은 이미 빠져나갔다")
        assertEquals(0, payments.countByOrderId(orderId), "우리 DB에는 아직 결제가 없다")
        val (recoveryPoint, locked) = jdbc.queryForMap(
            "SELECT recovery_point, locked_at IS NOT NULL AS locked FROM idempotency_keys WHERE idempotency_key = ?", key,
        ).let { it["recovery_point"] to it["locked"] }
        assertEquals("started", recoveryPoint)
        assertEquals(false, locked, "재시도 가능한 에러이므로 잠금이 풀려 있다")

        // 2차 시도(같은 key): recovery_point=started 부터 이어서 → PG를 다시 부르지만 같은 key라 PG가 기존 승인을 돌려준다
        pg.timeoutAfterCharge = false
        val retry = post(key, body)

        assertEquals(201, retry.statusCode.value())
        assertEquals(2, pg.callCount(), "PG는 두 번 호출됐지만")
        assertEquals(1, pg.chargeCount(), "실제 승인은 한 번뿐 (PG 멱등성)")
        assertEquals(1, payments.countByOrderId(orderId))
    }

    @Test
    fun missing_header__400() {
        val headers = HttpHeaders().apply { contentType = MediaType.APPLICATION_JSON }
        val response = http.postForEntity("/payments", HttpEntity(body(newOrderId(), 10_000), headers), String::class.java)

        assertEquals(400, response.statusCode.value())
        assertEquals(0, pg.callCount())
    }

    private fun post(key: String, body: String): ResponseEntity<String> {
        val headers = HttpHeaders().apply {
            contentType = MediaType.APPLICATION_JSON
            set("Idempotency-Key", key)
        }
        return http.postForEntity("/payments", HttpEntity(body, headers), String::class.java)
    }

    private fun body(orderId: String, amount: Long) = """{"orderId": "$orderId", "amount": $amount}"""
    private fun json(s: String?) = objectMapper.readTree(s)
    private fun newKey() = UUID.randomUUID().toString()
    private fun newOrderId() = "order-" + UUID.randomUUID()
}
