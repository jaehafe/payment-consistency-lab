package lab.recon

import org.springframework.core.io.ClassPathResource
import org.springframework.stereotype.Component

/**
 * recon_queries.sql 을 "-- name: xxx" 단위로 잘라 이름 → SQL 맵으로 들고 있는다.
 * SQL을 코드 문자열에 박지 않고 별도 파일로 두는 이유: 운영자가 같은 SQL을 그대로 DB에서 실행해 볼 수 있게.
 */
@Component
class ReconQueries {
    private val queries: Map<String, String> = parse(ClassPathResource("recon_queries.sql").inputStream.bufferedReader().readText())

    operator fun get(name: String): String = queries[name] ?: throw IllegalArgumentException("recon_queries.sql 에 없는 쿼리: $name")

    companion object {
        private val NAME = Regex("""^--\s*name:\s*(\S+)\s*$""")

        fun parse(text: String): Map<String, String> {
            val result = linkedMapOf<String, String>()
            var current: String? = null
            val buf = StringBuilder()
            fun flush() {
                current?.let { result[it] = buf.toString().trim().removeSuffix(";").trim() }
                buf.clear()
            }
            for (line in text.lines()) {
                val m = NAME.matchEntire(line)
                if (m != null) {
                    flush()
                    current = m.groupValues[1]
                } else if (current != null) {
                    buf.appendLine(line)
                }
            }
            flush()
            return result
        }
    }
}
