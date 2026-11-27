package main

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Shakirullah01/OnlineExam/result-service/resultpb"
)

//go:embed schema.sql
var schemaFile embed.FS

type app struct {
	resultpb.UnimplementedResultServer
	db     *pgxpool.Pool
	cache  *redis.Client
	events *kafka.Writer
}

type resultView struct {
	AttemptID      int64  `json:"attempt_id"`
	ExamID         int64  `json:"exam_id"`
	StudentID      int64  `json:"student_id"`
	Title          string `json:"title"`
	CorrectCount   int    `json:"correct_count"`
	TotalQuestions int    `json:"total_questions"`
	Percentage     int    `json:"percentage"`
}

func main() {
	log.Fatal(run(context.Background()))
}

func run(ctx context.Context) error {
	service, err := connect(ctx)
	if err != nil {
		return err
	}

	go service.listen(ctx, "exam.created", service.saveExamRef)
	go service.listen(ctx, "exam.submitted", service.saveSubmission)

	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer()
	resultpb.RegisterResultServer(grpcServer, service)
	go grpcServer.Serve(listener)

	server := &http.Server{Addr: ":8082", Handler: service.routes()}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		server.Shutdown(shutdownCtx)
		grpcServer.GracefulStop()
	}()

	log.Println("result-service http://localhost:8082  grpc://localhost:50051")
	err = server.ListenAndServe()
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

func connect(ctx context.Context) (*app, error) {
	db, err := pgxpool.New(ctx, "postgres://admin:admin@localhost:5432/result_db?sslmode=disable")
	if err != nil {
		return nil, err
	}
	if err := db.Ping(ctx); err != nil {
		return nil, err
	}
	if err := createTables(ctx, db); err != nil {
		return nil, err
	}

	cache := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	if err := cache.Ping(ctx).Err(); err != nil {
		return nil, err
	}

	broker, err := kafka.Dial("tcp", "localhost:9092")
	if err != nil {
		return nil, err
	}
	broker.Close()

	return &app{
		db:    db,
		cache: cache,
		events: &kafka.Writer{
			Addr:                   kafka.TCP("localhost:9092"),
			AllowAutoTopicCreation: true,
			Balancer:               &kafka.LeastBytes{},
			BatchTimeout:           10 * time.Millisecond,
		},
	}, nil
}

func createTables(ctx context.Context, db *pgxpool.Pool) error {
	raw, err := schemaFile.ReadFile("schema.sql")
	if err != nil {
		return err
	}
	for _, part := range strings.Split(string(raw), ";") {
		stmt := strings.TrimSpace(part)
		if stmt == "" {
			continue
		}
		if _, err := db.Exec(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func (a *app) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", a.health)
	mux.HandleFunc("GET /results", a.listResults)
	mux.HandleFunc("GET /results/{id}", a.getResult)
	return cors(mux)
}

func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (a *app) health(w http.ResponseWriter, r *http.Request) {
	if err := a.db.Ping(r.Context()); err != nil {
		http.Error(w, "database is down", http.StatusServiceUnavailable)
		return
	}
	w.Write([]byte("ok"))
}

func (a *app) CalculateResult(ctx context.Context, req *resultpb.CalculateRequest) (*resultpb.CalculateResponse, error) {
	if req.GetAttemptId() <= 0 || req.GetExamId() <= 0 || req.GetStudentId() <= 0 || len(req.GetAnswers()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "attempt, exam, student, and answers are required")
	}

	answers := make([]checkedAnswer, 0, len(req.GetAnswers()))
	for _, answer := range req.GetAnswers() {
		if answer.GetSelectedOption() < 1 || answer.GetSelectedOption() > 4 || answer.GetCorrectOption() < 1 || answer.GetCorrectOption() > 4 {
			return nil, status.Error(codes.InvalidArgument, "options must be 1, 2, 3, or 4")
		}
		answers = append(answers, checkedAnswer{
			Selected: int(answer.GetSelectedOption()),
			Correct:  int(answer.GetCorrectOption()),
		})
	}

	correct, total, percentage := grade(answers)
	_, err := a.db.Exec(ctx,
		`INSERT INTO results (attempt_id, exam_id, student_id, correct_count, total_questions, percentage)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (attempt_id) DO NOTHING`,
		req.GetAttemptId(), req.GetExamId(), req.GetStudentId(), correct, total, percentage,
	)
	if err != nil {
		log.Println(err)
		return nil, status.Error(codes.Internal, "could not save result")
	}

	err = a.db.QueryRow(ctx,
		`SELECT correct_count, total_questions, percentage
		 FROM results
		 WHERE attempt_id = $1`,
		req.GetAttemptId(),
	).Scan(&correct, &total, &percentage)
	if err != nil {
		log.Println(err)
		return nil, status.Error(codes.Internal, "could not save result")
	}

	var title string
	_ = a.db.QueryRow(ctx, `SELECT title FROM exam_refs WHERE exam_id = $1`, req.GetExamId()).Scan(&title)
	view := resultView{
		AttemptID:      req.GetAttemptId(),
		ExamID:         req.GetExamId(),
		StudentID:      req.GetStudentId(),
		Title:          title,
		CorrectCount:   correct,
		TotalQuestions: total,
		Percentage:     percentage,
	}
	if body, err := json.Marshal(view); err == nil {
		if err := a.cache.Set(ctx, resultKey(req.GetAttemptId()), body, 0).Err(); err != nil {
			log.Println(err)
		}
	}

	a.publish(ctx, "result.calculated", map[string]any{"attempt_id": req.GetAttemptId()})

	return &resultpb.CalculateResponse{
		CorrectCount:   int32(correct),
		TotalQuestions: int32(total),
		Percentage:     int32(percentage),
	}, nil
}

func (a *app) listResults(w http.ResponseWriter, r *http.Request) {
	studentID, err := strconv.ParseInt(r.URL.Query().Get("student_id"), 10, 64)
	if err != nil || studentID <= 0 {
		http.Error(w, "student_id must be a positive number", http.StatusBadRequest)
		return
	}

	rows, err := a.db.Query(r.Context(),
		`SELECT r.attempt_id, r.exam_id, r.student_id, COALESCE(e.title, ''),
		        r.correct_count, r.total_questions, r.percentage
		 FROM results r
		 LEFT JOIN exam_refs e ON e.exam_id = r.exam_id
		 WHERE r.student_id = $1
		 ORDER BY r.attempt_id`,
		studentID,
	)
	if err != nil {
		log.Println(err)
		http.Error(w, "could not load results", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	list := []resultView{}
	for rows.Next() {
		var view resultView
		if err := rows.Scan(&view.AttemptID, &view.ExamID, &view.StudentID, &view.Title, &view.CorrectCount, &view.TotalQuestions, &view.Percentage); err != nil {
			log.Println(err)
			http.Error(w, "could not load results", http.StatusInternalServerError)
			return
		}
		list = append(list, view)
	}
	if err := rows.Err(); err != nil {
		log.Println(err)
		http.Error(w, "could not load results", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, list)
}

func (a *app) getResult(w http.ResponseWriter, r *http.Request) {
	attemptID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || attemptID <= 0 {
		http.Error(w, "attempt id must be a positive number", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	cached, err := a.cache.Get(ctx, resultKey(attemptID)).Bytes()
	if err == nil {
		w.Header().Set("Content-Type", "application/json")
		w.Write(cached)
		return
	}
	if err != redis.Nil {
		log.Println(err)
	}

	view, err := a.loadResult(ctx, attemptID)
	if errors.Is(err, pgx.ErrNoRows) {
		http.Error(w, "result not found", http.StatusNotFound)
		return
	}
	if err != nil {
		log.Println(err)
		http.Error(w, "could not load result", http.StatusInternalServerError)
		return
	}

	body, err := json.Marshal(view)
	if err != nil {
		log.Println(err)
		http.Error(w, "could not load result", http.StatusInternalServerError)
		return
	}
	if err := a.cache.Set(ctx, resultKey(attemptID), body, 0).Err(); err != nil {
		log.Println(err)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(body)
}

func (a *app) loadResult(ctx context.Context, attemptID int64) (resultView, error) {
	var view resultView
	err := a.db.QueryRow(ctx,
		`SELECT r.attempt_id, r.exam_id, r.student_id, COALESCE(e.title, ''),
		        r.correct_count, r.total_questions, r.percentage
		 FROM results r
		 LEFT JOIN exam_refs e ON e.exam_id = r.exam_id
		 WHERE r.attempt_id = $1`,
		attemptID,
	).Scan(&view.AttemptID, &view.ExamID, &view.StudentID, &view.Title, &view.CorrectCount, &view.TotalQuestions, &view.Percentage)
	return view, err
}

func resultKey(attemptID int64) string {
	return fmt.Sprintf("result:attempt:%d", attemptID)
}

func writeJSON(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(value)
}
