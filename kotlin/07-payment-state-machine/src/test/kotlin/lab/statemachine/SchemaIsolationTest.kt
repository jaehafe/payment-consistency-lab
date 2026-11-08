package lab.statemachine

import com.zaxxer.hikari.HikariDataSource
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.core.io.FileSystemResource
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator
import java.io.File
import java.util.UUID
import javax.sql.DataSource

/**
 * Kotlin 단계들을 같은 DB(compose의 `lab`)에서 차례로 bootRun 해도 테이블이 섞이지 않는지.
 *
 * 각 단계는 같은 이름의 테이블을 다른 컬럼으로 만든다 (`payments`는 01~04, 07, 10이 전부 다르다).
 * 모두 public에 만들면 먼저 뜬 단계의 테이블이 남고, 뒤 단계의 `CREATE TABLE IF NOT EXISTS`는 그냥 넘어간다.
 * → 03을 띄운 DB에서 07을 띄우면 07의 schema.sql이 `next_retry_at` 인덱스에서 깨져 앱이 안 뜬다.
 *
 * 여기서는 다른 모듈의 앱을 띄울 수 없으므로, 각 모듈이 기동할 때 하는 일을 그대로 흉내 낸다:
 * 그 모듈의 application.yml 에 있는 `spring.datasource.hikari.schema` 로 커넥션 풀을 만들고
 * (앱과 같은 설정), 그 풀로 그 모듈의 schema.sql 을 실행한다 (spring.sql.init 과 같은 ScriptUtils 분할).
 * 매 테스트는 새 데이터베이스에서 시작한다 — 이 테스트 클래스의 앱이 이미 만든 테이블과 섞이지 않게.
 */
@SpringBootTest(properties = ["lab.janitor.enabled=false"])
@Import(TestcontainersConfiguration::class)
class SchemaIsolationTest {
    @Autowired lateinit var dataSource: DataSource
    @Autowired lateinit var jdbc: JdbcTemplate

    private val modules = listOf(
        "01-dual-write-problem", "02-idempotency-key", "03-outbox-polling", "04-outbox-cdc",
        "05-idempotent-consumer", "06-balance-concurrency", "07-payment-state-machine",
        "08-ledger-settlement", "09-reconciliation", "10-observability",
    )

    /** @ServiceConnection 이 URL을 컨테이너로 바꿔도 hikari.schema 는 그대로 적용된다 → 테스트도 앱과 같은 스키마를 쓴다. */
    @Test
    fun app_datasource__uses_k07_schema() {
        assertEquals("k07", jdbc.queryForObject("SELECT current_schema()", String::class.java))
        assertEquals("k07", jdbc.queryForObject("SELECT schemaname FROM pg_tables WHERE tablename = 'payment_status_history'", String::class.java))
    }

    /** 요청의 대표 사례: 03 bootRun → 07 bootRun (03의 payments 에는 attempt_count / next_retry_at 이 없다). */
    @Test
    fun boot_03_then_07__same_db__07_schema_applies_and_07_insert_works() {
        withFreshDatabase { url ->
            boot("03-outbox-polling", url).use { ds03 ->
                JdbcTemplate(ds03).update(
                    "INSERT INTO payments (id, order_id, amount, status) VALUES (?, 'o-03', 1000, 'COMPLETED')", UUID.randomUUID(),
                )
            }
            boot("07-payment-state-machine", url).use { ds07 -> insert07Payment(JdbcTemplate(ds07)) }
        }
    }

    /** 01 → 10 을 모두 같은 DB에 차례로 띄워도 각 단계의 테이블이 자기 스키마에 자기 컬럼으로 있다. */
    @Test
    fun boot_all_stages_in_order__same_db__every_table_lives_in_its_stage_schema() {
        withFreshDatabase { url ->
            val schemaOf = modules.associateWith { m -> boot(m, url).use { schemaSetting(m) ?: "public" } }

            val admin = JdbcTemplate(dataSource(url, null))
            try {
                for (m in modules) {
                    val created = tablesIn(m).sorted()
                    val found = admin.queryForList(
                        "SELECT tablename FROM pg_tables WHERE schemaname = ? ORDER BY 1", String::class.java, schemaOf.getValue(m),
                    )
                    assertEquals(created, found.filter { it in created }, "$m 의 테이블이 스키마 ${schemaOf[m]} 에 있어야 한다")
                }
                // 스키마 이름이 단계끼리 겹치면 격리가 아니다
                assertEquals(modules.size, schemaOf.values.toSet().size, "단계마다 스키마가 달라야 한다: $schemaOf")
            } finally {
                (admin.dataSource as HikariDataSource).close()
            }
            boot("07-payment-state-machine", url).use { insert07Payment(JdbcTemplate(it)) }
        }
    }

    private fun insert07Payment(jdbc: JdbcTemplate) {
        jdbc.update(
            "INSERT INTO payments (id, order_id, amount, status, idempotency_key, attempt_count, next_retry_at) " +
                "VALUES (?, 'o-07', 1000, 'UNKNOWN', 'k-07', 1, now())",
            UUID.randomUUID(),
        )
    }

    /** 모듈 하나의 기동을 흉내: 그 모듈 설정의 커넥션 풀 + 그 모듈의 schema.sql. */
    private fun boot(module: String, url: String): HikariDataSource {
        val ds = dataSource(url, schemaSetting(module))
        ResourceDatabasePopulator(FileSystemResource(schemaFile(module))).execute(ds)
        return ds
    }

    private fun schemaSetting(module: String): String? =
        YamlPropertiesFactoryBean().apply { setResources(FileSystemResource("../$module/src/main/resources/application.yml")) }
            .getObject()!!.getProperty("spring.datasource.hikari.schema")

    private fun schemaFile(module: String) = File("../$module/src/main/resources/schema.sql")

    /** schema.sql 의 `CREATE TABLE IF NOT EXISTS <name>` 목록 */
    private fun tablesIn(module: String): List<String> =
        Regex("""CREATE TABLE IF NOT EXISTS\s+(\w+)""", RegexOption.IGNORE_CASE)
            .findAll(schemaFile(module).readText()).map { it.groupValues[1] }.toList()

    private fun dataSource(url: String, schema: String?): HikariDataSource {
        val base = dataSource as HikariDataSource
        return HikariDataSource().apply {
            jdbcUrl = url
            username = base.username
            password = base.password
            maximumPoolSize = 2
            this.schema = schema // 앱의 spring.datasource.hikari.schema 와 같은 경로 (Connection.setSchema)
        }
    }

    private fun withFreshDatabase(block: (String) -> Unit) {
        val name = "iso_" + UUID.randomUUID().toString().replace("-", "").take(12)
        jdbc.execute("CREATE DATABASE $name")
        try {
            block((dataSource as HikariDataSource).jdbcUrl.replace(Regex("""/[^/?]+(\?|$)"""), "/$name$1"))
        } finally {
            jdbc.execute("DROP DATABASE IF EXISTS $name WITH (FORCE)")
        }
    }
}
