package lab.ledger

/** 계정과목. id 접두사로 유형이 정해진다. */
object Accounts {
    const val PG_RECEIVABLE = "pg:receivable"          // 자산: PG가 우리에게 줄 돈
    const val BANK_CASH = "bank:cash"                  // 자산: 은행 잔고
    const val FEE_REVENUE = "platform:fee_revenue"     // 수익: 우리 수수료
    fun merchantPayable(merchantId: String) = "merchant:$merchantId:payable" // 부채: 가맹점에 줄 돈

    fun typeOf(id: String): String = when {
        id == PG_RECEIVABLE || id == BANK_CASH -> "ASSET"
        id == FEE_REVENUE -> "REVENUE"
        id.startsWith("merchant:") && id.endsWith(":payable") -> "LIABILITY"
        else -> throw IllegalArgumentException("알 수 없는 계정: $id")
    }
}

/** 분개의 한 줄. 차변(debit) 또는 대변(credit) 한쪽만 양수. */
data class Line(val accountId: String, val debit: Long = 0, val credit: Long = 0) {
    companion object {
        fun debit(accountId: String, amount: Long) = Line(accountId, debit = amount)
        fun credit(accountId: String, amount: Long) = Line(accountId, credit = amount)
    }
}

/** 분개(전표). 같은 (referenceType, referenceId)는 한 번만 전기된다. */
data class JournalEntry(
    val referenceType: String,   // PAYMENT | REFUND | PG_DEPOSIT | PAYOUT
    val referenceId: String,
    val description: String,
    val lines: List<Line>,
)

class UnbalancedEntryException(message: String) : RuntimeException(message)
