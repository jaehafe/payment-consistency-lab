package lab.consumer

import org.slf4j.LoggerFactory
import org.springframework.context.SmartLifecycle
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * 앱 실행 시 ConsumerLoop를 백그라운드 스레드에서 계속 돌린다.
 * (IdempotentConsumerTest는 pollOnce를 직접 호출하고, DeadLetterTest는 이걸 돌린다)
 *
 * 실패가 이어지면 기다리는 시간을 retryDelay, 2배, 4배, ... maxRetryDelay(기본 30초)까지
 * 늘린다 (지수 백오프, Spring ExponentialBackOff multiplier 2 + maxInterval).
 * 메시지를 하나라도 처리하면 다시 retryDelay부터.
 *  - DB가 잠깐 죽었다면(TRANSIENT) 몇 초~수십 초 간격으로 계속 두드리다 살아나면 처리한다.
 *    DeadLetterPublisher가 횟수를 세지 않으므로 DLT로 가지 않는다.
 *  - poison message는 DeadLetterPublisher가 maxAttempts번째에 DLT로 보내고 넘어간다.
 */
class ConsumerRunner(
    private val retryDelay: Duration = Duration.ofSeconds(1),
    private val maxRetryDelay: Duration = DEFAULT_MAX_RETRY_DELAY,
    private val loopFactory: () -> ConsumerLoop,
) : SmartLifecycle {
    private val log = LoggerFactory.getLogger(javaClass)
    @Volatile private var running = false
    private var thread: Thread? = null
    private val stopped = CountDownLatch(1) // 백오프 대기 중에도 stop()이 바로 깨운다

    init {
        require(!retryDelay.isNegative && !retryDelay.isZero) {
            "retryDelay must be > 0, got $retryDelay"
        }
        require(maxRetryDelay >= retryDelay) {
            "maxRetryDelay($maxRetryDelay) must be >= retryDelay($retryDelay)"
        }
    }

    override fun start() {
        running = true
        thread = Thread({
            var failures = 0 // 연속 실패 횟수
            while (running) {
                try {
                    loopFactory().use { loop ->
                        while (running) {
                            if (loop.pollOnce() > 0) failures = 0
                        }
                    }
                } catch (e: Exception) {
                    // 처리 실패 시 같은 컨슈머로 계속 poll 하면 안 된다: 메모리상의 position은 이미 실패한 메시지
                    // 뒤로 넘어가 있어서, 다음 commitSync가 실패한 메시지까지 "처리됨"으로 커밋해 버린다(유실).
                    // 그래서 컨슈머를 닫고 새로 만든다 → 마지막으로 커밋된 offset부터 다시 받는다.
                    // 같은 메시지가 계속 실패하면 ConsumerLoop의 DeadLetterPublisher가
                    // maxAttempts번째에 DLT로 보내고 넘어간다. 실패 횟수는 새로 만드는 루프가 아니라
                    // loopFactory 밖에서 한 번 만든 DeadLetterPublisher에 쌓인다.
                    failures++
                    val wait = backoff(retryDelay, maxRetryDelay, failures)
                    log.error(
                        "메시지 처리 실패 (연속 {}번), {}ms 뒤 컨슈머를 재생성해 마지막 커밋 지점부터 다시 받는다",
                        failures, wait.toMillis(), e,
                    )
                    stopped.await(wait.toMillis(), TimeUnit.MILLISECONDS)
                }
            }
        }, "payment-events-consumer").apply { start() }
    }

    override fun stop() {
        running = false
        stopped.countDown()
        thread?.join(5_000)
    }

    override fun isRunning() = running

    companion object {
        val DEFAULT_MAX_RETRY_DELAY: Duration = Duration.ofSeconds(30)

        /** 연속 failures번째(1부터) 실패 뒤 기다릴 시간: base·2^(failures-1), 상한 max */
        fun backoff(base: Duration, max: Duration, failures: Int): Duration {
            var d = base
            repeat(failures - 1) {
                d = d.multipliedBy(2)
                if (d >= max) return max
            }
            return if (d > max) max else d
        }
    }
}
