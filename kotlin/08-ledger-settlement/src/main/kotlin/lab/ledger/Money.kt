package lab.ledger

import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId

object Fees {
    /**
     * 수수료 = amount × rateBps / 10000, 원 단위 반올림(HALF_UP).
     * 정수 연산만 쓴다: +5000 을 더하고 나누면 소수점 첫째 자리 반올림과 같다. (31.5 → 32, 31.47 → 31)
     * Double을 쓰면 반올림이 틀어질 수 있다: 5,750원 × 2.2% = 126.5 → 127원이어야 하는데
     * 5750 * 0.022 = 126.49999999999999 (0.022를 2진수로 정확히 표현 못 함) → Math.round = 126원.
     * (1050 * 0.03은 우연히 정확히 31.5가 나와서 예시로 부적절하다)
     */
    fun calculate(amount: Long, rateBps: Long): Long = (amount * rateBps + 5_000) / 10_000
}

/** 한국 가맹점의 "거래일"은 KST 기준이다. DB에는 UTC(timestamptz)로 저장되어 있으므로 경계를 변환해야 한다. */
object Kst {
    val ZONE: ZoneId = ZoneId.of("Asia/Seoul")

    /** KST 날짜 하루 = [그날 00:00 KST, 다음날 00:00 KST). 예: 11-08 KST = [11-07 15:00Z, 11-08 15:00Z) */
    fun dayRange(date: LocalDate): Pair<Instant, Instant> =
        date.atStartOfDay(ZONE).toInstant() to date.plusDays(1).atStartOfDay(ZONE).toInstant()
}
