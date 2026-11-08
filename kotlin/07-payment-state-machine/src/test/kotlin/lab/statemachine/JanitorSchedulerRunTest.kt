package lab.statemachine

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.Mockito

/**
 * 스프링 없이 run() 한 바퀴만 검증: 한쪽 작업이 예외를 던져도
 * run()은 예외를 밖으로 던지지 않고(스케줄러가 죽지 않음),
 * 나머지 작업(취소 재시도)도 실행한다.
 */
class JanitorSchedulerRunTest {
    @Test
    fun exception_in_one_task__does_not_escape_or_skip_other() {
        // reconcileUnknown은 항상 실패, retryCancelRequested는 2건 처리
        val janitor = Mockito.mock(Janitor::class.java) { inv ->
            when (inv.method.name) {
                "reconcileUnknown" -> throw IllegalStateException("DB down")
                "retryCancelRequested" -> 2
                else -> null
            }
        }
        val scheduler = JanitorScheduler(janitor)

        scheduler.run() // 던지면 테스트 실패
        scheduler.run() // 다음 바퀴도 정상 실행

        val called = Mockito.mockingDetails(janitor).invocations
            .map { it.method.name }
        assertEquals(
            listOf(
                "reconcileUnknown", "retryCancelRequested",
                "reconcileUnknown", "retryCancelRequested",
            ),
            called,
        )
    }
}
