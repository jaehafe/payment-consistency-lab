package lab.observability

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Component
import java.time.Duration

/**
 * 결제 메트릭. Micrometer 이름(점 표기) → Prometheus 이름 변환:
 *   payment.requests        (Counter) → payment_requests_total{result=...}
 *   pg.call.duration        (Timer)   → pg_call_duration_seconds_bucket/_count/_sum  (histogram)
 *   payment.unknown.current (Gauge)   → payment_unknown_current
 *   idempotency.replays     (Counter) → idempotency_replays_total
 * 이 이름들은 infra/observability/alerts.yml 의 알림 규칙이 그대로 쓰므로 바꾸면 알림이 조용히 죽는다.
 */
@Component
class PaymentMetrics(
    private val registry: MeterRegistry,
    jdbc: JdbcTemplate,
) {
    enum class Result { APPROVED, FAILED, UNKNOWN, REPLAYED, CONFLICT }

    private val requests: Map<Result, Counter> = Result.entries.associateWith { r ->
        // 앱 시작 시 모든 result 라벨을 0으로 미리 등록한다.
        // 한 번도 증가하지 않은 라벨은 시계열이 아예 없어서, 실패율 = failed / total 같은 PromQL이 "값 없음"이 된다.
        Counter.builder("payment.requests")
            .description("결제 요청 결과별 건수")
            .tag("result", r.name.lowercase())
            .register(registry)
    }

    private val replays: Counter = Counter.builder("idempotency.replays")
        .description("같은 멱등키 재요청에 저장된 결과를 돌려준 횟수")
        .register(registry)

    init {
        // UNKNOWN 결제 수는 "지금 이 순간의 상태"이므로 Gauge. 스크레이프할 때마다 DB에서 센다.
        // (앱 메모리 카운터로 세면 재시작 시 0이 되고, 여러 인스턴스 값이 서로 다르다. DB가 진실이다)
        Gauge.builder("payment.unknown.current") {
            jdbc.queryForObject("SELECT count(*) FROM payments WHERE status = 'UNKNOWN'", Long::class.java)!!.toDouble()
        }
            .description("UNKNOWN 상태 결제 수 (janitor가 확정해야 함)")
            .register(registry)
    }

    fun request(result: Result) = requests.getValue(result).increment()

    fun replayed() = replays.increment()

    /** PG 호출 시간. outcome(success|declined|timeout)으로 나눠 "느린 성공"과 "타임아웃"을 구분해 볼 수 있다. */
    fun recordPgCall(outcome: String, duration: Duration) {
        Timer.builder("pg.call.duration")
            .description("PG 승인 API 호출 시간")
            .tag("outcome", outcome)
            .publishPercentileHistogram() // _bucket 시계열 생성 → PromQL histogram_quantile(0.99, ...) 가능
            .minimumExpectedValue(Duration.ofMillis(5))
            .maximumExpectedValue(Duration.ofSeconds(30))
            .register(registry) // 같은 이름+태그면 기존 Timer를 돌려준다
            .record(duration)
    }
}
