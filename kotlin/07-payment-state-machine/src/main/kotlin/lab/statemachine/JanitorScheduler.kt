package lab.statemachine

import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.annotation.Scheduled
import java.time.Instant

/**
 * Janitor를 앱 안에서 주기 실행한다.
 * killbill의 Janitor.start()가
 * janitorExecutor.scheduleAtFixedRate(task, period, period, unit)
 * 하는 것과 같은 역할.
 *
 * lab.janitor.enabled=true 일 때만 빈이 생기고, 그때만
 * @EnableScheduling 도 켜진다. 테스트는 시각(now)을 직접 넘겨
 * janitor를 부르므로 끈다 (스케줄러가 끼어들면 결정적이지 않다).
 *
 * fixedDelay vs fixedRate
 * - fixedRate: "이전 실행의 시작" 기준 주기. 한 번이 주기보다
 *   오래 걸리면 끝나자마자 다음 실행이 바로 붙는다(밀린 만큼 연달아).
 *   killbill이 쓰는 방식 (기본 1h, 단일 스레드 executor).
 * - fixedDelay: "이전 실행의 끝" 기준. 끝난 뒤 항상 interval을 쉰다.
 * 여기선 fixedDelay를 고른다:
 *   1) 한 바퀴의 시간은 PG 조회 속도에 달렸다. PG가 느릴 때
 *      fixedRate는 쉬지 않고 연달아 돌아 장애 중인 PG를 더 두드린다.
 *   2) 무엇을 처리할지는 next_retry_at(지수 백오프)이 정한다.
 *      주기가 정확할 필요가 없고, 주기는 "얼마나 자주 확인하나"일 뿐.
 *   3) Go 버전(Janitor.Run, time.After)과 같은 의미가 된다.
 * 어느 쪽이든 Spring 기본 스케줄러는 같은 메서드를 겹쳐 실행하지 않는다.
 * 인스턴스가 여러 대여도 claimDue(SKIP LOCKED + lease) 덕분에 안전하다.
 */
@Configuration(proxyBeanMethods = false)
@EnableScheduling
@ConditionalOnProperty(name = ["lab.janitor.enabled"], havingValue = "true")
class JanitorScheduler(private val janitor: Janitor) {
    private val log = LoggerFactory.getLogger(javaClass)

    /**
     * 한 바퀴: 오래된 PENDING 정리 → UNKNOWN 확정 → 취소 재시도.
     * PENDING을 먼저 본다: 판단 불가로 UNKNOWN에 넘긴 건은
     * next_retry_at(백오프 뒤)이 정하는 바퀴에 UNKNOWN 쪽이 다시 본다.
     * 예외는 여기서 잡는다. (Spring도 반복 작업의 예외는 로그만 남기고
     * 다음 실행을 계속하지만, 한쪽 실패가 다른 쪽을 건너뛰게 하지 않고
     * 요약 로그를 남기기 위해 직접 잡는다. killbill도 항목별로
     * catch(Exception) → log.warn 하고 루프를 계속한다.)
     */
    @Scheduled(
        initialDelayString = "\${lab.janitor.interval}",
        fixedDelayString = "\${lab.janitor.interval}",
    )
    fun run() {
        val now = Instant.now()
        val pending = safely("sweepPending") {
            janitor.sweepPending(now)
        }
        val unknown = safely("reconcileUnknown") {
            janitor.reconcileUnknown(now)
        }
        val cancels = safely("retryCancelRequested") {
            janitor.retryCancelRequested(now)
        }
        // 요약: 이번에 집어 간(확인한) 건수. 실패한 쪽은 error
        val summary = "janitor run: pending=${pending ?: "error"}" +
            " unknown=${unknown ?: "error"}" +
            " cancelRequested=${cancels ?: "error"}"
        // 할 일이 없던 바퀴는 DEBUG (주기마다 INFO면 로그가 넘친다)
        val busy = listOf(pending, unknown, cancels).any { (it ?: 1) > 0 }
        if (busy) log.info(summary) else log.debug(summary)
    }

    /** 예외를 로그로 바꾸고 null을 돌려준다 (Error는 잡지 않는다). */
    private fun safely(name: String, block: () -> Int): Int? =
        try {
            block()
        } catch (e: Exception) {
            log.warn("janitor: {} 실패 (다음 주기에 재시도)", name, e)
            null
        }
}
