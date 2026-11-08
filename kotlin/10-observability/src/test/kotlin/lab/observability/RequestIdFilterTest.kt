package lab.observability

import jakarta.servlet.FilterChain
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.slf4j.MDC
import org.springframework.mock.web.MockHttpServletRequest
import org.springframework.mock.web.MockHttpServletResponse

/**
 * 필터를 직접 호출하는 단위 테스트 (스프링/DB 없음).
 * 서블릿 스레드 재사용을 "같은 테스트 스레드에서 요청 두 번"으로 결정적으로 재현한다.
 */
class RequestIdFilterTest {
    private val filter = RequestIdFilter()

    @Test
    fun mdc_cleared_between_requests_on_same_thread() {
        // 요청 1: 결제 처리 중 서비스가 MDC에 payment_id/order_id를 넣는다
        filter.doFilter(request("req-1"), MockHttpServletResponse(), FilterChain { _, _ ->
            MDC.put(Mdc.PAYMENT_ID, "pay-1")
            MDC.put(Mdc.ORDER_ID, "order-1")
            MDC.put(Mdc.IDEMPOTENCY_KEY, "key-1")
        })

        // 요청 2: 같은 스레드에서 처리되는 400 (헤더 누락 → 서비스까지 안 감, MDC를 아무도 안 채움)
        var seen: Map<String, String>? = null
        val response = MockHttpServletResponse()
        filter.doFilter(request(null), response, FilterChain { _, res ->
            (res as MockHttpServletResponse).status = 400
            seen = MDC.getCopyOfContextMap()
        })

        val ctx = seen!!
        assertNull(ctx[Mdc.PAYMENT_ID], "이전 요청의 payment_id가 새어 들어오면 안 된다: $ctx")
        assertNull(ctx[Mdc.ORDER_ID], "이전 요청의 order_id: $ctx")
        assertNull(ctx[Mdc.IDEMPOTENCY_KEY], "이전 요청의 idempotency_key: $ctx")
        assertNotEquals("req-1", ctx[Mdc.REQUEST_ID])
        assertEquals(response.getHeader(RequestIdFilter.HEADER), ctx[Mdc.REQUEST_ID])
    }

    private fun request(requestId: String?) = MockHttpServletRequest("POST", "/payments").apply {
        requestId?.let { addHeader(RequestIdFilter.HEADER, it) }
    }
}
