import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    id("org.springframework.boot") version "3.5.16" apply false
    id("io.spring.dependency-management") version "1.1.7" apply false
    kotlin("jvm") version "2.2.21" apply false
    kotlin("plugin.spring") version "2.2.21" apply false
}

// 모든 단계 모듈에 공통인 설정만 여기 둔다.
// (모듈별 의존성 — web, kafka 등 — 은 각 모듈의 build.gradle.kts에서 추가)
subprojects {
    apply(plugin = "org.springframework.boot")
    apply(plugin = "io.spring.dependency-management")
    apply(plugin = "org.jetbrains.kotlin.jvm")
    apply(plugin = "org.jetbrains.kotlin.plugin.spring")

    group = "lab"
    version = "0.0.1"

    repositories { mavenCentral() }

    extensions.configure<org.jetbrains.kotlin.gradle.dsl.KotlinJvmProjectExtension> {
        jvmToolchain(21)
        compilerOptions {
            jvmTarget.set(JvmTarget.JVM_21)
            freeCompilerArgs.addAll("-Xjsr305=strict", "-Xannotation-default-target=param-property")
        }
    }

    dependencies {
        // JPA 없이 JdbcTemplate + TransactionTemplate만 쓴다 — 트랜잭션 경계를 코드에서 눈으로 보기 위해.
        "implementation"("org.springframework.boot:spring-boot-starter-jdbc")
        "implementation"("org.springframework.boot:spring-boot-starter-json") // ObjectMapper 자동 설정 (Instant → ISO-8601)
        "implementation"("com.fasterxml.jackson.module:jackson-module-kotlin")
        "implementation"("org.jetbrains.kotlin:kotlin-reflect")
        "runtimeOnly"("org.postgresql:postgresql")

        "testImplementation"("org.springframework.boot:spring-boot-starter-test")
        "testImplementation"("org.springframework.boot:spring-boot-testcontainers")
        "testImplementation"("org.testcontainers:junit-jupiter")
        "testImplementation"("org.testcontainers:postgresql")
        "testImplementation"("org.jetbrains.kotlin:kotlin-test-junit5")
        "testRuntimeOnly"("org.junit.platform:junit-platform-launcher")
    }

    tasks.withType<Test> {
        useJUnitPlatform()
        testLogging {
            events("passed", "failed", "skipped")
            exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
        }
    }

    // 학습용 모듈이므로 실행 가능한 boot jar만 만들고 plain jar는 생략
    tasks.named<Jar>("jar") { enabled = false }
}
