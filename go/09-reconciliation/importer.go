package recon

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// csvHeader 는 PG 정산 파일의 기대 헤더.
var csvHeader = []string{"pg_tx_id", "order_id", "type", "amount", "fee", "transacted_at"}

// ImportResult 는 import 결과.
type ImportResult struct {
	FileID  int64
	Rows    int
	Skipped bool // 같은 파일(해시 동일)을 이미 import 했으면 true
}

// Importer 는 PG 정산 CSV 를 DB 로 옮긴다.
type Importer struct {
	DB *pgxpool.Pool
}

// Import 는 파일 하나를 import 한다. 같은 내용의 파일이 다시 오면 무시한다 (파일 단위 멱등성).
//
// 왜 해시인가? PG 가 같은 파일을 재전송하거나, 배치가 재시도되면 같은 파일이 두 번 들어온다.
// 파일 이름은 바뀔 수 있으므로 "내용"의 SHA-256 으로 판단한다.
// 단, 원본 바이트가 아니라 정규화한 내용(normalizeCSV)의 해시다.
func (im *Importer) Import(ctx context.Context, fileName string, content []byte) (ImportResult, error) {
	// 엑셀이 붙이는 UTF-8 BOM 은 해시/파싱 모두에서 뗀다
	content = bytes.TrimPrefix(content, []byte("\xEF\xBB\xBF"))
	// 해시와 파싱이 같은 입력을 보게 한다 (Kotlin 도 같다)
	normalized := normalizeCSV(content)
	sum := sha256.Sum256([]byte(normalized))
	hash := hex.EncodeToString(sum[:]) // [32]byte → 16진수 문자열

	records, err := parseCSV([]byte(normalized))
	if err != nil {
		return ImportResult{}, fmt.Errorf("%s: %w", fileName, err)
	}

	tx, err := im.DB.Begin(ctx)
	if err != nil {
		return ImportResult{}, err
	}
	defer tx.Rollback(ctx)

	var fileID int64
	// RETURNING id 와 ON CONFLICT DO NOTHING 을 같이 쓰면: 새로 들어갔을 때만 id 가 돌아오고, 충돌이면 "행 없음".
	err = tx.QueryRow(ctx, `
		INSERT INTO recon_files (file_name, file_hash, row_count) VALUES ($1, $2, $3)
		ON CONFLICT (file_hash) DO NOTHING
		RETURNING id`, fileName, hash, len(records)).Scan(&fileID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ImportResult{Skipped: true}, nil // 이미 import 한 파일
	}
	if err != nil {
		return ImportResult{}, err
	}

	// CopyFrom: Postgres COPY 프로토콜로 대량 insert (행마다 INSERT 보다 훨씬 빠르다. JDBC 의 batchUpdate 보다도 빠름)
	rows := make([][]any, len(records)) // [][]any = 행들의 목록, 각 행은 값들의 목록 (Kotlin의 List<List<Any?>>)
	for i, r := range records {
		rows[i] = []any{fileID, r.PgTxID, r.OrderID, r.Type, r.Amount, r.Fee, r.TransactedAt}
	}
	if _, err := tx.CopyFrom(ctx,
		pgx.Identifier{"pg_settlement_records"},
		[]string{"file_id", "pg_tx_id", "order_id", "type", "amount", "fee", "transacted_at"},
		pgx.CopyFromRows(rows)); err != nil {
		return ImportResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ImportResult{}, err
	}
	return ImportResult{FileID: fileID, Rows: len(records)}, nil
}

// normalizeCSV 는 의미 없는 차이를 지운 텍스트를 만든다.
// 같은 파일이 CRLF 로 다시 저장되거나 끝 개행이 하나 더 붙어 오면
// 원본 바이트 해시는 달라져서 "새 파일"로 들어가고,
// 전 건이 DUPLICATE_IN_PG 가 된다. 그래서 해시 전에:
//   - CRLF, CR → LF
//   - 줄 앞뒤 공백 trim
//   - 빈 줄 제거 → 끝 개행 개수 무관
//
// 파싱도 이 결과로 한다 → "해시가 같으면 읽는 데이터도 같다".
// (Kotlin 의 lines().map { trim() }.filter { isNotEmpty() } 와 같다)
func normalizeCSV(content []byte) string {
	s := strings.ReplaceAll(string(content), "\r\n", "\n")
	s = strings.ReplaceAll(s, "\r", "\n")
	var lines []string
	for _, l := range strings.Split(s, "\n") {
		if l = strings.TrimSpace(l); l != "" {
			lines = append(lines, l)
		}
	}
	return strings.Join(lines, "\n")
}

// pgRecord 는 CSV 한 줄.
type pgRecord struct {
	PgTxID, OrderID, Type string
	Amount, Fee           int64
	TransactedAt          time.Time
}

// parseCSV 는 encoding/csv 로 파일을 읽고 각 필드를 타입에 맞게 변환한다.
// 금액은 strconv.ParseInt 로 정수 파싱 — "10000.0" 같은 값은 거부한다 (금액에 소수점이 오면 파일 오류로 본다).
func parseCSV(content []byte) ([]pgRecord, error) {
	r := csv.NewReader(bytes.NewReader(content)) // []byte 를 io.Reader 로 감싼다 (Kotlin의 ByteArrayInputStream)
	header, err := r.Read()
	if err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	if !slices.Equal(header, csvHeader) {
		return nil, fmt.Errorf("unexpected header %v, want %v", header, csvHeader)
	}

	var out []pgRecord
	for line := 2; ; line++ { // 조건 없는 for = 무한 루프. break/return 으로 빠져나간다.
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", line, err)
		}
		amount, err := strconv.ParseInt(rec[3], 10, 64) // (문자열, 진법, 비트 수)
		if err != nil {
			return nil, fmt.Errorf("line %d amount: %w", line, err)
		}
		fee, err := strconv.ParseInt(rec[4], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("line %d fee: %w", line, err)
		}
		at, err := time.Parse(time.RFC3339, rec[5]) // 예: 2026-11-08T10:00:00+09:00
		if err != nil {
			return nil, fmt.Errorf("line %d transacted_at: %w", line, err)
		}
		out = append(out, pgRecord{PgTxID: rec[0], OrderID: rec[1], Type: rec[2], Amount: amount, Fee: fee, TransactedAt: at})
	}
	return out, nil
}
