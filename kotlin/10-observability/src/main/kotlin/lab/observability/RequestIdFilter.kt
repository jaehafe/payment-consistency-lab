package lab.observability

import jakarta.servlet.FilterChain
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.springframework.core.Ordered
import org.springframework.core.annotation.Order
import org.springframework.stereotype.Component
import org.springframework.web.filter.OncePerRequestFilter
import java.util.UUID

/**
 * 요청마다 request_id를 정해 MDC에 넣는다 → 이 요청 처리 중 찍히는 "모든" 로그에 자동으로 붙는다.
 * - 클라이언트/게이트웨이가 X-Request-Id를 보내면 그대로 쓴다 (서비스 경계를 넘어 같은 id로 추적)
 * - 없으면 생성하고, 응답 헤더로 돌려준다 (고객 문의 시 이 id 하나로 로그를 찾는다)
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
class RequestIdFilter : OncePerRequestFilter() {
    private val log = LoggerFactory.getLogger(javaClass)
    private val safe = Regex("^[A-Za-z0-9._-]{1,64}$") // 헤더 값을 그대로 로그에 넣으므로 로그 인젝션 방지

    override fun doFilterInternal(request: HttpServletRequest, response: HttpServletResponse, chain: FilterChain) {
        val requestId = request.getHeader(HEADER)?.takeIf { safe.matches(it) } ?: UUID.randomUUID().toString()
        val started = System.nanoTime()
        MDC.put(Mdc.REQUEST_ID, requestId)
        response.setHeader(HEADER, requestId)
        try {
            chain.doFilter(request, response)
        } finally {
            log.info(
                "요청 완료 {} {} status={} duration_ms={}",
                request.method, request.requestURI, response.status, (System.nanoTime() - started) / 1_000_000,
            )
            // 서블릿 스레드는 풀에서 재사용된다. 지우지 않으면 다음 요청 로그에 이전 요청의 id가 섞인다.
            MDC.clear()
        }
    }

    companion object {
        const val HEADER = "X-Request-Id"
    }
}

/** MDC 키 (= JSON 로그 필드명). Go 구현의 slog 필드명과 같다. */
object Mdc {
    const val REQUEST_ID = "request_id"
    const val PAYMENT_ID = "payment_id"
    const val ORDER_ID = "order_id"
    const val IDEMPOTENCY_KEY = "idempotency_key"
}
