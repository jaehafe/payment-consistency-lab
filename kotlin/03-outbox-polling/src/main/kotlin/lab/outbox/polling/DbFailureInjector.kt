package lab.outbox.polling

import org.springframework.dao.DataAccessResourceFailureException
import org.springframework.stereotype.Component

/** "커밋 직전에 DB가 실패하는" 상황을 흉내내는 토글. 트랜잭션 마지막 줄에서 호출 → 예외 → 롤백. */
@Component
class DbFailureInjector {
    @Volatile
    var failBeforeCommit = false

    fun maybeFail() {
        if (failBeforeCommit) throw DataAccessResourceFailureException("DB 장애 주입: 커밋 직전 실패")
    }
}
