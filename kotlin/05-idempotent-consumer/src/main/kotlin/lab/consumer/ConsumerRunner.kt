package lab.consumer

import org.slf4j.LoggerFactory
import org.springframework.context.SmartLifecycle

/** 앱 실행 시 ConsumerLoop를 백그라운드 스레드에서 계속 돌린다. (테스트는 이걸 쓰지 않고 pollOnce를 직접 호출) */
class ConsumerRunner(private val loopFactory: () -> ConsumerLoop) : SmartLifecycle {
    private val log = LoggerFactory.getLogger(javaClass)
    @Volatile private var running = false
    private var thread: Thread? = null

    override fun start() {
        running = true
        thread = Thread({
            while (running) {
                try {
                    loopFactory().use { loop -> while (running) loop.pollOnce() }
                } catch (e: Exception) {
                    // 처리 실패 시 같은 컨슈머로 계속 poll 하면 안 된다: 메모리상의 position은 이미 실패한 메시지
                    // 뒤로 넘어가 있어서, 다음 commitSync가 실패한 메시지까지 "처리됨"으로 커밋해 버린다(유실).
                    // 그래서 컨슈머를 닫고 새로 만든다 → 마지막으로 커밋된 offset부터 다시 받는다.
                    // (단순화: 실제로는 재시도 횟수 제한, DLQ(dead letter topic) 등이 필요하다)
                    log.error("메시지 처리 실패, 컨슈머를 재생성해 마지막 커밋 지점부터 다시 받는다", e)
                    Thread.sleep(1_000)
                }
            }
        }, "payment-events-consumer").apply { start() }
    }

    override fun stop() {
        running = false
        thread?.join(5_000)
    }

    override fun isRunning() = running
}
