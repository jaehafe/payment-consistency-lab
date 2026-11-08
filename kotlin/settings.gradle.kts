rootProject.name = "payment-consistency-lab-kotlin"

// 단계별 모듈. 각 모듈은 독립 실행 가능한 작은 Spring Boot 앱이다.
include(
    "01-dual-write-problem",
    "02-idempotency-key",
    "03-outbox-polling",
    "04-outbox-cdc",
    "05-idempotent-consumer",
)
