package lab.balance

import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * n개의 스레드를 준비시킨 뒤 "시작 래치"로 동시에 출발시킨다.
 * 각 작업의 결과(성공 값 또는 예외)를 순서대로 돌려준다.
 */
fun <T> runConcurrently(n: Int, task: (index: Int) -> T): List<Result<T>> {
    val pool = Executors.newFixedThreadPool(n)
    val ready = CountDownLatch(n)
    val start = CountDownLatch(1)
    try {
        val futures = (0 until n).map { i ->
            pool.submit<Result<T>> {
                ready.countDown()
                start.await() // 모든 스레드가 여기까지 온 뒤 한꺼번에 출발
                runCatching { task(i) }
            }
        }
        ready.await()
        start.countDown()
        return futures.map { it.get(60, TimeUnit.SECONDS) }
    } finally {
        pool.shutdown()
    }
}
