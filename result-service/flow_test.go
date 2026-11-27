package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"github.com/Shakirullah01/OnlineExam/result-service/resultpb"
)

var (
	testDB    *pgxpool.Pool
	testCache *redis.Client
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, "postgres://admin:admin@localhost:5432/postgres?sslmode=disable")
	if err != nil {
		os.Exit(m.Run())
	}
	_, err = admin.Exec(ctx, "CREATE DATABASE result_test")
	var pgErr *pgconn.PgError
	if err != nil && (!errors.As(err, &pgErr) || pgErr.Code != "42P04") {
		admin.Close()
		os.Exit(m.Run())
	}
	admin.Close()

	testDB, err = pgxpool.New(ctx, "postgres://admin:admin@localhost:5432/result_test?sslmode=disable")
	if err != nil {
		os.Exit(m.Run())
	}
	if err := createTables(ctx, testDB); err != nil {
		testDB.Close()
		testDB = nil
		os.Exit(m.Run())
	}
	testCache = redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 2})
	if err := testCache.Ping(ctx).Err(); err != nil {
		testCache = nil
	}
	code := m.Run()
	if testDB != nil {
		testDB.Close()
	}
	os.Exit(code)
}

func newTestApp(t *testing.T) *app {
	t.Helper()
	if testDB == nil || testCache == nil {
		t.Skip("postgres or redis is not running")
	}
	ctx := context.Background()
	if err := testCache.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	_, err := testDB.Exec(ctx, `TRUNCATE results, submission_log, exam_refs`)
	if err != nil {
		t.Fatal(err)
	}
	return &app{db: testDB, cache: testCache}
}

func call(a *app, method, path string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	rec := httptest.NewRecorder()
	a.routes().ServeHTTP(rec, req)
	return rec
}

func TestCalculateListAndCache(t *testing.T) {
	a := newTestApp(t)
	a.saveExamRef([]byte(`{"exam_id":4,"title":"math","teacher_id":1}`))
	a.saveExamRef([]byte(`not json`))
	a.saveSubmission([]byte(`not json`))
	a.saveSubmission([]byte(`{"attempt_id":8,"exam_id":4,"student_id":3,"submitted_at":"2026-01-02T03:04:05Z"}`))

	var savedTitle string
	if err := testDB.QueryRow(context.Background(), `SELECT title FROM exam_refs WHERE exam_id = 4`).Scan(&savedTitle); err != nil {
		t.Fatal(err)
	}
	if savedTitle != "math" {
		t.Fatal(savedTitle)
	}

	first, err := a.CalculateResult(context.Background(), &resultpb.CalculateRequest{
		AttemptId: 8,
		ExamId:    4,
		StudentId: 3,
		Answers: []*resultpb.Answer{
			{SelectedOption: 1, CorrectOption: 1},
			{SelectedOption: 2, CorrectOption: 3},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if first.GetCorrectCount() != 1 || first.GetTotalQuestions() != 2 || first.GetPercentage() != 50 {
		t.Fatalf("%+v", first)
	}

	second, err := a.CalculateResult(context.Background(), &resultpb.CalculateRequest{
		AttemptId: 8,
		ExamId:    4,
		StudentId: 3,
		Answers: []*resultpb.Answer{
			{SelectedOption: 1, CorrectOption: 1},
			{SelectedOption: 3, CorrectOption: 3},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if second.GetPercentage() != 50 {
		t.Fatalf("score changed to %d", second.GetPercentage())
	}

	if _, err := a.CalculateResult(context.Background(), &resultpb.CalculateRequest{}); err == nil {
		t.Fatal("empty request should fail")
	}
	if _, err := a.CalculateResult(context.Background(), &resultpb.CalculateRequest{
		AttemptId: 1,
		ExamId:    1,
		StudentId: 1,
		Answers:   []*resultpb.Answer{{SelectedOption: 9, CorrectOption: 1}},
	}); err == nil {
		t.Fatal("bad option should fail")
	}

	if rec := call(a, http.MethodGet, "/health"); rec.Body.String() != "ok" {
		t.Fatal(rec.Body.String())
	}
	opt := httptest.NewRequest(http.MethodOptions, "/health", nil)
	optRec := httptest.NewRecorder()
	a.routes().ServeHTTP(optRec, opt)
	if optRec.Code != http.StatusNoContent {
		t.Fatalf("options %d", optRec.Code)
	}
	if err := testCache.Del(context.Background(), "result:attempt:8").Err(); err != nil {
		t.Fatal(err)
	}
	got := call(a, http.MethodGet, "/results/8")
	if got.Code != http.StatusOK || !strings.Contains(got.Body.String(), `"percentage":50`) || !strings.Contains(got.Body.String(), `"title":"math"`) {
		t.Fatalf("result %d %s", got.Code, got.Body.String())
	}
	cached := call(a, http.MethodGet, "/results/8")
	if cached.Body.String() != got.Body.String() {
		t.Fatalf("cache read %s", cached.Body.String())
	}
	if _, err := testCache.Get(context.Background(), "result:attempt:8").Result(); err != nil {
		t.Fatal(err)
	}

	if rec := call(a, http.MethodGet, "/results/99"); rec.Code != http.StatusNotFound {
		t.Fatalf("missing %d", rec.Code)
	}
	if rec := call(a, http.MethodGet, "/results/0"); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad id %d", rec.Code)
	}
	list := call(a, http.MethodGet, "/results?student_id=3")
	if list.Code != http.StatusOK || !strings.Contains(list.Body.String(), "math") {
		t.Fatalf("list %s", list.Body.String())
	}
	if rec := call(a, http.MethodGet, "/results?student_id=0"); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad student %d", rec.Code)
	}
	empty := call(a, http.MethodGet, "/results?student_id=100")
	if strings.TrimSpace(empty.Body.String()) != "[]" {
		t.Fatalf("empty list %s", empty.Body.String())
	}

	when := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	var gotTime time.Time
	if err := testDB.QueryRow(context.Background(), `SELECT submitted_at FROM submission_log WHERE attempt_id = 8`).Scan(&gotTime); err != nil {
		t.Fatal(err)
	}
	if !gotTime.Equal(when) {
		t.Fatalf("submitted_at %s", gotTime)
	}
}

func TestPublishWithoutKafka(t *testing.T) {
	a := newTestApp(t)
	a.publish(context.Background(), "result.calculated", map[string]any{"attempt_id": 1})
	a.publish(context.Background(), "result.calculated", map[string]any{"bad": func() {}})
	a.events = &kafka.Writer{
		Addr:         kafka.TCP("127.0.0.1:1"),
		WriteTimeout: 200 * time.Millisecond,
		BatchTimeout: 10 * time.Millisecond,
	}
	a.publish(context.Background(), "result.calculated", map[string]any{"attempt_id": 1})

	broker, err := kafka.Dial("tcp", "localhost:9092")
	if err != nil {
		return
	}
	broker.Close()
	a.events = &kafka.Writer{
		Addr:                   kafka.TCP("localhost:9092"),
		AllowAutoTopicCreation: true,
		Balancer:               &kafka.LeastBytes{},
		BatchTimeout:           10 * time.Millisecond,
	}
	a.publish(context.Background(), "coverage.check", map[string]int{"ok": 1})
}

func TestClosedDatabaseReturnsErrors(t *testing.T) {
	if testCache == nil {
		t.Skip("redis is not running")
	}
	db, err := pgxpool.New(context.Background(), "postgres://admin:admin@localhost:5432/result_test?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	a := &app{db: db, cache: testCache}

	if rec := call(a, http.MethodGet, "/results?student_id=1"); rec.Code != http.StatusInternalServerError {
		t.Fatalf("list %d", rec.Code)
	}
	if rec := call(a, http.MethodGet, "/results/123456"); rec.Code != http.StatusInternalServerError {
		t.Fatalf("one result %d", rec.Code)
	}
	_, err = a.CalculateResult(context.Background(), &resultpb.CalculateRequest{
		AttemptId: 123456,
		ExamId:    1,
		StudentId: 1,
		Answers:   []*resultpb.Answer{{SelectedOption: 1, CorrectOption: 1}},
	})
	if err == nil {
		t.Fatal("grading should fail when the database is closed")
	}
	a.saveExamRef([]byte(`{"exam_id":123456,"title":"x","teacher_id":1}`))
	a.saveSubmission([]byte(`{"attempt_id":123456,"exam_id":1,"student_id":1,"submitted_at":"2026-01-02T03:04:05Z"}`))
}

func TestHealthWhenDatabaseIsClosed(t *testing.T) {
	if testDB == nil {
		t.Skip("postgres is not running")
	}
	db, err := pgxpool.New(context.Background(), "postgres://admin:admin@localhost:5432/result_test?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	rec := call(&app{db: db}, http.MethodGet, "/health")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("health %d", rec.Code)
	}
}

func TestServiceStartsAndStops(t *testing.T) {
	if testDB == nil || testCache == nil {
		t.Skip("postgres or redis is not running")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- run(ctx)
	}()

	deadline := time.Now().Add(8 * time.Second)
	for {
		resp, err := http.Get("http://127.0.0.1:8082/health")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("result-service did not start")
		}
		time.Sleep(50 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("result-service did not stop")
	}
}
