package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/Shakirullah01/OnlineExam/result-service/resultpb"
)

var (
	testDB     *pgxpool.Pool
	testCache  *redis.Client
	testEvents *kafka.Writer
	testGrader resultpb.ResultClient
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, "postgres://admin:admin@localhost:5432/postgres?sslmode=disable")
	if err != nil {
		os.Exit(m.Run())
	}
	_, err = admin.Exec(ctx, "CREATE DATABASE exam_test")
	var pgErr *pgconn.PgError
	if err != nil && (!errors.As(err, &pgErr) || pgErr.Code != "42P04") {
		admin.Close()
		os.Exit(m.Run())
	}
	admin.Close()

	testDB, err = pgxpool.New(ctx, "postgres://admin:admin@localhost:5432/exam_test?sslmode=disable")
	if err != nil {
		os.Exit(m.Run())
	}
	if err := createTables(ctx, testDB); err != nil {
		testDB.Close()
		testDB = nil
		os.Exit(m.Run())
	}

	testCache = redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 1})
	if err := testCache.Ping(ctx).Err(); err != nil {
		testCache = nil
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err == nil {
		server := grpc.NewServer()
		resultpb.RegisterResultServer(server, &stubGrader{})
		go server.Serve(listener)
		conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			testGrader = resultpb.NewResultClient(conn)
		}
	}

	code := m.Run()
	if testDB != nil {
		testDB.Close()
	}
	os.Exit(code)
}

var stubShouldFail bool

type stubGrader struct {
	resultpb.UnimplementedResultServer
}

func (s *stubGrader) CalculateResult(ctx context.Context, req *resultpb.CalculateRequest) (*resultpb.CalculateResponse, error) {
	if stubShouldFail {
		return nil, status.Error(codes.Unavailable, "grader is down")
	}
	var correct int32
	for _, answer := range req.GetAnswers() {
		if answer.GetSelectedOption() == answer.GetCorrectOption() {
			correct++
		}
	}
	total := int32(len(req.GetAnswers()))
	percentage := int32(0)
	if total > 0 {
		percentage = correct * 100 / total
	}
	return &resultpb.CalculateResponse{
		CorrectCount:   correct,
		TotalQuestions: total,
		Percentage:     percentage,
	}, nil
}

func newTestApp(t *testing.T) *app {
	t.Helper()
	if testDB == nil || testCache == nil || testGrader == nil {
		t.Skip("postgres, redis, or the test grader is not running")
	}
	if err := testCache.FlushDB(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	_, err := testDB.Exec(context.Background(), `TRUNCATE attempt_answers, attempts, options, questions, exams RESTART IDENTITY`)
	if err != nil {
		t.Fatal(err)
	}
	return &app{db: testDB, cache: testCache, events: testEvents, grader: testGrader}
}

func call(a *app, method, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	a.routes().ServeHTTP(rec, req)
	return rec
}

func TestHealthAndCreateList(t *testing.T) {
	a := newTestApp(t)

	if rec := call(a, http.MethodGet, "/health", ""); rec.Code != http.StatusOK || rec.Body.String() != "ok" {
		t.Fatalf("health %d %s", rec.Code, rec.Body.String())
	}
	if rec := call(a, http.MethodPost, "/exams", "{"); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad json %d", rec.Code)
	}
	if rec := call(a, http.MethodPost, "/exams", `{"title":"","teacher_id":1,"duration_minutes":10,"questions":[]}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid exam %d", rec.Code)
	}

	created := call(a, http.MethodPost, "/exams", oneQuestionExam("math"))
	if created.Code != http.StatusCreated {
		t.Fatalf("create %d %s", created.Code, created.Body.String())
	}
	if strings.Contains(created.Body.String(), "correct_option") {
		t.Fatal("create response leaked the correct option")
	}

	listed := call(a, http.MethodGet, "/exams", "")
	if listed.Code != http.StatusOK || !strings.Contains(listed.Body.String(), "math") {
		t.Fatalf("list %s", listed.Body.String())
	}
	cached, err := testCache.Get(context.Background(), "exams:all").Result()
	if err != nil || !strings.Contains(cached, "math") {
		t.Fatalf("cache %v %s", err, cached)
	}

	again := call(a, http.MethodGet, "/exams", "")
	if again.Body.String() != listed.Body.String() {
		t.Fatalf("second list changed: %s", again.Body.String())
	}

	second := call(a, http.MethodPost, "/exams", oneQuestionExam("history"))
	if second.Code != http.StatusCreated {
		t.Fatal(second.Body.String())
	}
	if _, err := testCache.Get(context.Background(), "exams:all").Result(); err != redis.Nil {
		t.Fatalf("cache should be cleared, got %v", err)
	}
}

func TestStartSubmitAndGrade(t *testing.T) {
	a := newTestApp(t)
	created := call(a, http.MethodPost, "/exams", twoQuestionExam())
	if created.Code != http.StatusCreated {
		t.Fatal(created.Body.String())
	}

	if rec := call(a, http.MethodPost, "/exams/99/start", `{"student_id":1}`); rec.Code != http.StatusNotFound {
		t.Fatalf("missing exam %d", rec.Code)
	}
	if rec := call(a, http.MethodPost, "/exams/1/start", `{"student_id":0}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad student %d", rec.Code)
	}

	started := call(a, http.MethodPost, "/exams/1/start", `{"student_id":7}`)
	if started.Code != http.StatusOK {
		t.Fatalf("start %d %s", started.Code, started.Body.String())
	}
	if strings.Contains(started.Body.String(), "correct_option") {
		t.Fatal("start response leaked the correct option")
	}

	var body struct {
		AttemptID int64 `json:"attempt_id"`
		Questions []struct {
			ID      int64 `json:"id"`
			Options []struct {
				Number int `json:"number"`
			} `json:"options"`
		} `json:"questions"`
	}
	if err := json.Unmarshal(started.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Questions) != 2 || len(body.Questions[0].Options) != 4 {
		t.Fatalf("questions %#v", body.Questions)
	}

	again := call(a, http.MethodPost, "/exams/1/start", `{"student_id":7}`)
	if again.Code != http.StatusOK || !strings.Contains(again.Body.String(), `"attempt_id":`) {
		t.Fatalf("restart %d %s", again.Code, again.Body.String())
	}

	if rec := call(a, http.MethodPost, "/attempts/99/submit", `{"answers":[]}`); rec.Code != http.StatusNotFound {
		t.Fatalf("missing attempt %d", rec.Code)
	}
	if rec := call(a, http.MethodPost, "/attempts/1/submit", `{"answers":[]}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("empty answers %d %s", rec.Code, rec.Body.String())
	}

	// Question 1 correct option is 1. Question 2 correct option is 3.
	submitBody := `{"answers":[` +
		`{"question_id":` + strconv.FormatInt(body.Questions[0].ID, 10) + `,"selected_option":1},` +
		`{"question_id":` + strconv.FormatInt(body.Questions[1].ID, 10) + `,"selected_option":1}` +
		`]}`
	submitted := call(a, http.MethodPost, "/attempts/1/submit", submitBody)
	if submitted.Code != http.StatusOK {
		t.Fatalf("submit %d %s", submitted.Code, submitted.Body.String())
	}
	if !strings.Contains(submitted.Body.String(), `"correct_count":1`) || !strings.Contains(submitted.Body.String(), `"percentage":50`) {
		t.Fatalf("score %s", submitted.Body.String())
	}

	var status string
	if err := testDB.QueryRow(context.Background(), `SELECT status FROM attempts WHERE id = 1`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "submitted" {
		t.Fatalf("status %s", status)
	}

	a.markGraded(context.Background(), []byte(`{"attempt_id":1}`))
	if err := testDB.QueryRow(context.Background(), `SELECT status FROM attempts WHERE id = 1`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "graded" {
		t.Fatalf("after event status %s", status)
	}
	a.markGraded(context.Background(), []byte(`{"attempt_id":1}`))
	a.markGraded(context.Background(), []byte(`not json`))
	a.markGraded(context.Background(), []byte(`{"attempt_id":999999}`))

	if rec := call(a, http.MethodPost, "/attempts/1/submit", submitBody); rec.Code != http.StatusConflict {
		t.Fatalf("second submit %d", rec.Code)
	}
	if rec := call(a, http.MethodPost, "/exams/1/start", `{"student_id":7}`); rec.Code != http.StatusConflict {
		t.Fatalf("start after submit %d", rec.Code)
	}
}

func TestErrorPaths(t *testing.T) {
	a := newTestApp(t)
	if rec := call(a, http.MethodOptions, "/exams", ""); rec.Code != http.StatusNoContent {
		t.Fatalf("options %d", rec.Code)
	}
	if rec := call(a, http.MethodPost, "/exams/abc/start", `{"student_id":1}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad exam id %d", rec.Code)
	}
	if rec := call(a, http.MethodPost, "/exams/1/start", `{`); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad start json %d", rec.Code)
	}
	if rec := call(a, http.MethodPost, "/attempts/abc/submit", `{"answers":[]}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad attempt id %d", rec.Code)
	}
	if rec := call(a, http.MethodPost, "/attempts/1/submit", `{`); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad submit json %d", rec.Code)
	}

	closedCache := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 1})
	closedCache.Close()
	withClosedCache := &app{db: testDB, cache: closedCache, events: testEvents, grader: testGrader}
	if rec := call(withClosedCache, http.MethodGet, "/exams", ""); rec.Code != http.StatusOK {
		t.Fatalf("list with closed cache %d %s", rec.Code, rec.Body.String())
	}
	if rec := call(withClosedCache, http.MethodPost, "/exams", oneQuestionExam("closed-cache")); rec.Code != http.StatusCreated {
		t.Fatalf("create with closed cache %d %s", rec.Code, rec.Body.String())
	}

	db, err := pgxpool.New(context.Background(), "postgres://admin:admin@localhost:5432/exam_test?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	closed := &app{db: db, cache: testCache, events: testEvents, grader: testGrader}
	if rec := call(closed, http.MethodGet, "/health", ""); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("health %d", rec.Code)
	}
	if rec := call(closed, http.MethodGet, "/exams", ""); rec.Code != http.StatusInternalServerError {
		t.Fatalf("list %d", rec.Code)
	}
	if rec := call(closed, http.MethodPost, "/exams", oneQuestionExam("no-db")); rec.Code != http.StatusInternalServerError {
		t.Fatalf("create %d", rec.Code)
	}
	if rec := call(closed, http.MethodPost, "/exams/1/start", `{"student_id":1}`); rec.Code != http.StatusInternalServerError {
		t.Fatalf("start %d", rec.Code)
	}
	if rec := call(closed, http.MethodPost, "/attempts/1/submit", `{"answers":[]}`); rec.Code != http.StatusInternalServerError {
		t.Fatalf("submit %d", rec.Code)
	}
	closed.markGraded(context.Background(), []byte(`{"attempt_id":1}`))

	created := call(a, http.MethodPost, "/exams", oneQuestionExam("retry"))
	if created.Code != http.StatusCreated {
		t.Fatal(created.Body.String())
	}
	if rec := call(a, http.MethodPost, "/exams/1/start", `{"student_id":4}`); rec.Code != http.StatusOK {
		t.Fatal(rec.Body.String())
	}
	var started struct {
		Questions []struct {
			ID int64 `json:"id"`
		} `json:"questions"`
	}
	if err := json.Unmarshal(call(a, http.MethodPost, "/exams/1/start", `{"student_id":4}`).Body.Bytes(), &started); err != nil {
		t.Fatal(err)
	}
	_, err = testDB.Exec(context.Background(),
		`INSERT INTO attempt_answers (attempt_id, question_id, selected_option) VALUES (1, $1, 1)`,
		started.Questions[0].ID,
	)
	if err != nil {
		t.Fatal(err)
	}
	body := `{"answers":[{"question_id":` + strconv.FormatInt(started.Questions[0].ID, 10) + `,"selected_option":1}]}`
	if rec := call(a, http.MethodPost, "/attempts/1/submit", body); rec.Code != http.StatusInternalServerError {
		t.Fatalf("duplicate answer %d %s", rec.Code, rec.Body.String())
	}

	stubShouldFail = true
	defer func() { stubShouldFail = false }()
	down := call(a, http.MethodPost, "/exams", oneQuestionExam("grader-down"))
	if down.Code != http.StatusCreated {
		t.Fatal(down.Body.String())
	}
	var downExam struct {
		ID int64 `json:"id"`
	}
	if err := json.Unmarshal(down.Body.Bytes(), &downExam); err != nil {
		t.Fatal(err)
	}
	startedDown := call(a, http.MethodPost, "/exams/"+strconv.FormatInt(downExam.ID, 10)+"/start", `{"student_id":5}`)
	if startedDown.Code != http.StatusOK {
		t.Fatal(startedDown.Body.String())
	}
	var second struct {
		AttemptID int64 `json:"attempt_id"`
		Questions []struct {
			ID int64 `json:"id"`
		} `json:"questions"`
	}
	if err := json.Unmarshal(startedDown.Body.Bytes(), &second); err != nil {
		t.Fatal(err)
	}
	body = `{"answers":[{"question_id":` + strconv.FormatInt(second.Questions[0].ID, 10) + `,"selected_option":1}]}`
	if rec := call(a, http.MethodPost, "/attempts/"+strconv.FormatInt(second.AttemptID, 10)+"/submit", body); rec.Code != http.StatusBadGateway {
		t.Fatalf("grader down %d %s", rec.Code, rec.Body.String())
	}

	a.publish(context.Background(), "exam.created", map[string]any{"bad": func() {}})
	a.events = &kafka.Writer{
		Addr:         kafka.TCP("127.0.0.1:1"),
		WriteTimeout: 200 * time.Millisecond,
		BatchTimeout: 10 * time.Millisecond,
	}
	a.publish(context.Background(), "exam.created", map[string]int{"exam_id": 1})

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

func TestStartWhenOptionsTableIsMissing(t *testing.T) {
	a := newTestApp(t)
	if rec := call(a, http.MethodPost, "/exams", oneQuestionExam("broken")); rec.Code != http.StatusCreated {
		t.Fatal(rec.Body.String())
	}
	if _, err := testDB.Exec(context.Background(), `DROP TABLE options`); err != nil {
		t.Fatal(err)
	}
	defer createTables(context.Background(), testDB)

	rec := call(a, http.MethodPost, "/exams/1/start", `{"student_id":9}`)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("start %d %s", rec.Code, rec.Body.String())
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
	var up bool
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://127.0.0.1:8081/health")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				up = true
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	if !up {
		t.Fatal("exam-service did not start")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("exam-service did not stop")
	}
}

func oneQuestionExam(title string) string {
	return `{"title":"` + title + `","teacher_id":1,"duration_minutes":15,"questions":[{"body":"2+2","correct_option":1,"options":["4","3","5","6"]}]}`
}

func twoQuestionExam() string {
	return `{"title":"quiz","teacher_id":1,"duration_minutes":15,"questions":[` +
		`{"body":"2+2","correct_option":1,"options":["4","3","5","6"]},` +
		`{"body":"color of the sky","correct_option":3,"options":["red","green","blue","yellow"]}` +
		`]}`
}
