package recon

import (
	_ "embed"
	"fmt"
	"strings"
)

// recon_queries.sql 을 통째로 문자열로 포함한다. SQL 을 Go 코드 안에 흩어놓지 않고,
// 사람이 따로 읽고 psql 에서 바로 실행해볼 수 있도록 독립된 파일로 둔다.
//
//go:embed recon_queries.sql
var reconQueriesSQL string

// queries 는 이름 → SQL. 패키지가 로드될 때 한 번 파싱한다.
var queries = parseNamedQueries(reconQueriesSQL)

// parseNamedQueries 는 "-- name: xxx" 로 구분된 SQL 파일을 map 으로 쪼갠다.
// (Kotlin/JVM 에서는 MyBatis XML 이나 jOOQ, Go 에서는 sqlc 같은 도구가 비슷한 일을 한다)
func parseNamedQueries(src string) map[string]string {
	out := map[string]string{}
	var name string
	var body strings.Builder // 문자열을 효율적으로 이어 붙이는 도구 (Kotlin의 StringBuilder)
	flush := func() {        // 함수 안의 함수(클로저). 바깥 변수 name, body 를 그대로 쓴다.
		if name != "" {
			out[name] = strings.TrimSpace(body.String())
		}
		body.Reset()
	}
	for _, line := range strings.Split(src, "\n") {
		if after, ok := strings.CutPrefix(strings.TrimSpace(line), "-- name:"); ok {
			flush()
			name = strings.TrimSpace(after)
			continue
		}
		if name != "" {
			body.WriteString(line)
			body.WriteString("\n")
		}
	}
	flush()
	return out
}

// query 는 이름으로 SQL 을 꺼낸다. 오타면 바로 패닉 (설정 버그는 빨리 드러나는 게 낫다).
func query(name string) string {
	q, ok := queries[name]
	if !ok {
		panic(fmt.Sprintf("recon_queries.sql 에 %q 쿼리가 없다", name))
	}
	return q
}
