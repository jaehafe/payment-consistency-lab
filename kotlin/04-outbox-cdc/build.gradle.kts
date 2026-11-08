// 04는 애플리케이션이 outbox insert까지만 한다 — Kafka 의존성이 없다는 점 자체가 학습 포인트.
// 발행은 Debezium(Kafka Connect)이 WAL을 읽어서 대신한다.
