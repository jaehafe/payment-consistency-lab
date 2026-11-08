// package 이름은 디렉토리 이름과 달라도 된다. "03-outbox-polling"은 식별자가 될 수 없으므로 outboxpolling으로 짓는다.
// (Kotlin의 package 선언과 비슷하지만, Go에서는 "디렉토리 하나 = 패키지 하나"다.)
package outboxpolling

import (
	"context"
	_ "embed" // 밑줄(_) import: 패키지를 직접 쓰진 않고 //go:embed 기능만 켜기 위해 가져온다.

	"github.com/jackc/pgx/v5/pgxpool"
)

// //go:embed 는 컴파일 시점에 파일 내용을 문자열 변수에 넣어준다.
// Kotlin으로 치면 resources/schema.sql 을 읽어 상수로 박아두는 것과 비슷하다.
//
//go:embed schema.sql
var SchemaSQL string

// ApplySchema 는 테이블을 만든다. 테스트와 cmd/main.go 에서 사용한다.
//
// Go 함수는 보통 첫 인자로 context.Context 를 받는다.
// ctx 는 "이 작업의 취소 신호/타임아웃"을 들고 다니는 객체다. (Kotlin 코루틴의 CoroutineContext + Job 취소와 비슷)
//
// 반환 타입 error: Go에는 예외(throw)가 없고, 에러를 "값"으로 돌려준다. nil 이면 성공.
func ApplySchema(ctx context.Context, db *pgxpool.Pool) error {
	_, err := db.Exec(ctx, SchemaSQL) // 반환값이 2개(결과, 에러). 결과는 안 쓰므로 _ 로 버린다.
	return err
}
