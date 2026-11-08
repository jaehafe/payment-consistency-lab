package lab.consumer

import org.springframework.dao.DataAccessResourceFailureException
import org.springframework.dao.RecoverableDataAccessException
import org.springframework.dao.TransientDataAccessException
import org.springframework.transaction.CannotCreateTransactionException
import java.net.SocketException
import java.net.SocketTimeoutException
import java.sql.SQLException
import java.sql.SQLNonTransientConnectionException
import java.sql.SQLRecoverableException
import java.sql.SQLTransientException
import kotlin.reflect.KClass

/** 처리 실패의 종류. DeadLetterPublisher가 DLT로 보낼지를 이걸로 정한다. */
enum class FailureKind {
    /**
     * 다시 하면 될 수도, 안 될 수도 있다 → maxAttempts번까지 세고 넘으면 DLT.
     * 깨진 JSON, 검증 실패, 그리고 "모르는 예외" 전부 (기본값 — FailureClassifier 설명).
     */
    RETRYABLE,

    /** 몇 번을 다시 해도 결과가 같다 → 1번 만에 DLT (Spring의 not-retryable) */
    NOT_RETRYABLE,

    /**
     * 메시지가 아니라 인프라(DB·네트워크)가 아프다. 메시지는 멀쩡하므로 DLT로
     * 보내지 않고 시도 횟수도 세지 않는다. ConsumerRunner가 지수 백오프로
     * 끝없이 재시도한다 (장애가 풀릴 때까지 파티션 정지).
     */
    TRANSIENT,
}

/**
 * Spring Kafka의 ExceptionClassifier(not-retryable 목록, 원인 체인까지 본다) +
 * FailedRecordTracker.setBackOffFunction(예외별 BackOff)을 손으로 옮긴 것.
 *
 * 판정 순서 (원인 체인 전체를 본다 — Spring의 setTraverseCauses(true)):
 *  1. NOT_RETRYABLE이 먼저 — 메시지 자체가 틀렸으면 인프라 상태와 무관하다.
 *  2. TRANSIENT — 아래 isTransient 목록.
 *  3. 나머지는 RETRYABLE.
 *
 * "모르는 예외 = RETRYABLE"인 이유 (Spring ExceptionClassifier의 기본값
 * defaultValue=true와 같은 선택):
 *  - 모르는 예외를 TRANSIENT로 두면, 특정 payload에서만 터지는 버그
 *    (NPE, 범위 밖 값, 제약 위반)가 파티션을 영원히 멈춘다. DLT가 막으려던 바로 그 상황이다.
 *  - RETRYABLE로 두면 최악이 "분류 못 한 인프라 예외로 멀쩡한 메시지가 DLT에 간다"인데,
 *    DLT에는 원래 key·value·헤더와 위치가 남으니 유실은 아니다. 그 경우는 isTransient에 추가한다.
 *
 * DataAccessException 전체를 TRANSIENT로 두지 않는 이유: DataIntegrityViolationException
 * (제약 위반, 값이 너무 김)이나 BadSqlGrammarException은 몇 번을 해도 똑같이 실패한다.
 */
object FailureClassifier {
    private val notRetryable: Set<KClass<out Throwable>> =
        setOf(MissingMessageIdException::class)

    fun classify(e: Throwable): FailureKind {
        val chain = causeChain(e)
        return when {
            chain.any { c -> notRetryable.any { it.isInstance(c) } } ->
                FailureKind.NOT_RETRYABLE
            chain.any(::isTransient) -> FailureKind.TRANSIENT
            else -> FailureKind.RETRYABLE
        }
    }

    /** e, e.cause, e.cause.cause, ... (순환 방지로 최대 32단계) */
    private fun causeChain(e: Throwable): List<Throwable> =
        generateSequence(e) { t -> t.cause?.takeIf { it !== t } }
            .take(32).toList()

    private fun isTransient(e: Throwable): Boolean = when (e) {
        // Spring이 "다시 하면 될 수 있다"고 분류한 것: 쿼리 타임아웃, 락 획득 실패,
        // 데드락(ConcurrencyFailure), 일시적 자원 부족
        is TransientDataAccessException, is RecoverableDataAccessException -> true
        // DB 연결 실패. 이름은 NonTransient 계열이지만 SQLState 08(연결 오류)이 이걸로
        // 번역되고, CannotGetJdbcConnectionException도 이 하위 클래스다.
        is DataAccessResourceFailureException -> true
        // TransactionTemplate이 커넥션을 못 얻으면 DataAccessException이 아니라 이걸 던진다
        // (DataSourceTransactionManager.doBegin). DB가 내려갔을 때 실제로 보이는 예외.
        is CannotCreateTransactionException -> true
        // JDBC 표준 분류 (Hikari 풀 타임아웃 = SQLTransientConnectionException)
        is SQLTransientException, is SQLRecoverableException,
        is SQLNonTransientConnectionException -> true
        // pgjdbc의 PSQLException은 SQLException을 바로 상속한다 → SQLState로 판단
        is SQLException -> transientSqlState(e.sqlState)
        // TCP 수준 (ConnectException은 SocketException 하위). IOException 전체는 안 된다:
        // Jackson의 JsonProcessingException도 IOException이다.
        is SocketException, is SocketTimeoutException -> true
        else -> false
    }

    /** https://www.postgresql.org/docs/16/errcodes-appendix.html */
    private fun transientSqlState(state: String?): Boolean {
        if (state == null) return false
        return state.startsWith("08") || // connection_exception
            state.startsWith("53") || // insufficient_resources
            state in setOf(
                "57P01", // admin_shutdown
                "57P02", // crash_shutdown
                "57P03", // cannot_connect_now (기동 중)
                "57014", // query_canceled (statement_timeout)
                "40001", // serialization_failure
                "40P01", // deadlock_detected
            )
    }
}
