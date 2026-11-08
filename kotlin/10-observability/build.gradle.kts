dependencies {
    implementation("org.springframework.boot:spring-boot-starter-web")
    // /actuator/prometheus 엔드포인트 + Micrometer → Prometheus 포맷
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("io.micrometer:micrometer-registry-prometheus")
}
