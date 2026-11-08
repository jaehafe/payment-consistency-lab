package idempotency

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"
)

const body = `{"orderId": "order-1", "amount": 10000}`

func Test_same_key_twice__charged_once__same_response(t *testing.T) {
	app := newTestApp(t)

	code1, body1 := app.post(t, "key-1", body)
	code2, body2 := app.post(t, "key-1", body) // 클라이언트가 응답을 못 받았다고 생각하고 재시도

	if code1 != http.StatusCreated || code2 != http.StatusCreated {
		t.Fatalf("둘 다 201 이어야 한다: %d, %d", code1, code2)
	}
	if string(body1) != string(body2) {
		t.Fatalf("응답 바디가 바이트 단위로 같아야 한다:\n%s\n%s", body1, body2)
	}
	r1, r2 := decodeResponse(t, body1), decodeResponse(t, body2)
	// 구조체는 == 로 필드 전체를 비교할 수 있다 (Kotlin data class 의 equals 와 같다)
	if r1 != r2 {
		t.Fatalf("같은 응답이어야 한다:\n%+v\n%+v", r1, r2)
	}
	if n := app.gateway.ChargeCount(); n != 1 {
		t.Fatalf("PG 승인은 1번이어야 하는데 %d번", n)
	}
}

func Test_same_key_different_body__422(t *testing.T) {
	app := newTestApp(t)

	if code, _ := app.post(t, "key-1", body); code != http.StatusCreated {
		t.Fatalf("첫 요청은 201: %d", code)
	}
	code, _ := app.post(t, "key-1", `{"orderId": "order-1", "amount": 99999}`) // 같은 key, 다른 금액
	if code != http.StatusUnprocessableEntity {
		t.Fatalf("422 를 기대했지만 %d", code)
	}
	if n := app.gateway.ChargeCount(); n != 1 {
		t.Fatalf("PG 승인은 1번이어야 하는데 %d번", n)
	}
}

func Test_concurrent_same_key__only_one_charge(t *testing.T) {
	app := newTestApp(t)
	// PG 응답을 느리게 해서, 첫 요청이 처리 중인 동안 나머지 요청이 확실히 겹치게 만든다.
	app.gateway.ChargeDelay = 300 * time.Millisecond

	const n = 10
	codes := make([]int, n) // make: 길이 n 짜리 슬라이스(가변 배열) 생성. Kotlin의 IntArray(n)
	bodies := make([][]byte, n)
	start := make(chan struct{}) // 출발 신호용 채널. close 하면 기다리던 모든 goroutine 이 동시에 깨어난다.

	// sync.WaitGroup: goroutine 여러 개가 다 끝날 때까지 기다리는 도구 (Kotlin의 coroutineScope { } / awaitAll() 와 비슷)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		// go 키워드: 이 함수를 새 goroutine(경량 스레드)에서 실행한다. Kotlin의 launch { } 에 해당.
		// (Go 1.22+ 부터 for 루프 변수 i 는 반복마다 새로 만들어지므로 goroutine 안에서 그대로 써도 안전하다.)
		go func() {
			defer wg.Done()
			<-start // 출발 신호까지 대기
			// 각 goroutine 은 자기 칸(i)에만 쓰므로 잠금 불필요
			codes[i], bodies[i] = app.post(t, "same-key", body)
		}()
	}
	close(start) // 10개 요청 동시 출발
	wg.Wait()    // 전부 끝날 때까지 대기

	created, conflict := 0, 0
	var paymentID string
	for i, c := range codes { // range: 인덱스와 값을 함께 순회 (Kotlin의 forEachIndexed)
		switch c {
		case http.StatusCreated:
			created++
			r := decodeResponse(t, bodies[i])
			if paymentID != "" && r.PaymentID != paymentID {
				t.Fatalf("201 응답들은 모두 같은 결제여야 한다")
			}
			paymentID = r.PaymentID
		case http.StatusConflict:
			conflict++
		default:
			t.Fatalf("예상 못한 상태 코드 %d: %s", c, bodies[i])
		}
	}
	t.Logf("201=%d, 409=%d", created, conflict)

	if got := app.gateway.ChargeCount(); got != 1 {
		t.Fatalf("동시 요청 %d개 중 PG 승인은 1번이어야 하는데 %d번", n, got)
	}
	if created < 1 || conflict < 1 {
		t.Fatalf("최소 1개의 201 과 1개 이상의 409 가 나와야 한다 (201=%d, 409=%d)", created, conflict)
	}
	if cnt, _ := CountPayments(context.Background(), testPool, "order-1"); cnt != 1 {
		t.Fatalf("payments 는 1행이어야 하는데 %d행", cnt)
	}
}

func Test_pg_timeout_after_charge__retry_resumes__no_double_charge(t *testing.T) {
	app := newTestApp(t)

	// 1차 시도: PG 는 승인했지만 응답이 유실되어 서버는 타임아웃만 본다.
	app.gateway.TimeoutAfterCharge.Store(true)
	code, _ := app.post(t, "key-1", body)
	if code != http.StatusServiceUnavailable {
		t.Fatalf("1차 시도는 503 이어야 한다: %d", code)
	}
	if n := app.gateway.ChargeCount(); n != 1 {
		t.Fatalf("PG 에서는 이미 1번 승인됐다: %d", n)
	}
	if cnt, _ := CountPayments(context.Background(), testPool, "order-1"); cnt != 0 {
		t.Fatalf("우리 DB 에는 아직 결제가 없다: %d", cnt)
	}

	// 2차 시도: 네트워크 복구 후 클라이언트가 같은 key 로 재시도.
	// recovery_point=started 에서 이어서 PG 를 다시 부르지만, PG 멱등성 덕분에 새 승인 없이 기존 pgTxId 를 받는다.
	app.gateway.TimeoutAfterCharge.Store(false)
	code, b := app.post(t, "key-1", body)
	if code != http.StatusCreated {
		t.Fatalf("재시도는 201 이어야 한다: %d %s", code, b)
	}
	if n := app.gateway.ChargeCount(); n != 1 {
		t.Fatalf("이중 결제 발생! PG 승인 %d번", n)
	}
	if r := decodeResponse(t, b); r.PgTxID == "" {
		t.Fatal("pgTxId 가 있어야 한다")
	}
}

func Test_missing_header__400(t *testing.T) {
	app := newTestApp(t)
	code, _ := app.post(t, "", body)
	if code != http.StatusBadRequest {
		t.Fatalf("400 을 기대했지만 %d", code)
	}
	if n := app.gateway.ChargeCount(); n != 0 {
		t.Fatalf("PG 는 호출되지 않아야 한다: %d", n)
	}
}
