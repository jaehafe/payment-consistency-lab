package lab.dualwrite

import org.springframework.dao.DataAccessResourceFailureException
import org.springframework.stereotype.Component

/**
 * "커밋 직전에 DB가 실패하는" 상황을 흉내내는 토글.
 * 트랜잭션 블록의 마지막 줄에서 maybeFail()을 호출 → 예외 → TransactionTemplate이 롤백한다.
 * (실제로는 커넥션 끊김, 제약조건 위반, 데드락, 디스크 풀 등)
 */
@Component
class DbFailureInjector {
    @Volatile
    var failBeforeCommit = false

    fun maybeFail() {
        if (failBeforeCommit) throw DataAccessResourceFailureException("DB 장애 주입: 커밋 직전 실패")
    }
}
