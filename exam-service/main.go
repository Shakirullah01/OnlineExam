package main

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Shakirullah01/OnlineExam/result-service/resultpb"
)

// schema.sql is packed into the program, so it runs no matter which folder we start from.
//
//go:embed schema.sql
var schemaFile embed.FS

type app struct {
	db     *pgxpool.Pool
	cache  *redis.Client
	events *kafka.Writer
	grader resultpb.ResultClient
}

func main() {
	log.Fatal(run(context.Background()))
}

func run(ctx context.Context) error {
	service, err := connect(ctx)
	if err != nil {
		return err
	}

	go service.listenForGrades(ctx)

	server := &http.Server{Addr: ":8081", Handler: service.routes()}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		server.Shutdown(shutdownCtx)
	}()

	log.Println("exam-service is listening on http://localhost:8081")
	err = server.ListenAndServe()
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

func connect(ctx context.Context) (*app, error) {
	db, err := pgxpool.New(ctx, "postgres://admin:admin@localhost:5432/exam_db?sslmode=disable")
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

	graderConn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	return &app{
		db:    db,
		cache: cache,
		events: &kafka.Writer{
			Addr:                   kafka.TCP("localhost:9092"),
			AllowAutoTopicCreation: true,
			Balancer:               &kafka.LeastBytes{},
			BatchTimeout:           10 * time.Millisecond,
		},
		grader: resultpb.NewResultClient(graderConn),
	}, nil
}

func createTables(ctx context.Context, db *pgxpool.Pool) error {
	raw, err := schemaFile.ReadFile("schema.sql")
	if err != nil {
		return err
	}

	// The driver runs one SQL command at a time, so we split the file on ';'.
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
	mux.HandleFunc("GET /exams", a.listExams)
	mux.HandleFunc("POST /exams", a.createExam)
	mux.HandleFunc("POST /exams/{id}/start", a.startAttempt)
	mux.HandleFunc("POST /attempts/{id}/submit", a.submitAttempt)
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

type examRequest struct {
	Title           string            `json:"title"`
	TeacherID       int64             `json:"teacher_id"`
	DurationMinutes int               `json:"duration_minutes"`
	Questions       []questionRequest `json:"questions"`
}

type questionRequest struct {
	Body          string   `json:"body"`
	CorrectOption int      `json:"correct_option"`
	Options       []string `json:"options"`
}

type examResponse struct {
	ID            int64  `json:"id"`
	Title         string `json:"title"`
	QuestionCount int    `json:"question_count"`
}

type examListItem struct {
	ID              int64  `json:"id"`
	Title           string `json:"title"`
	TeacherID       int64  `json:"teacher_id"`
	DurationMinutes int    `json:"duration_minutes"`
}

func (a *app) listExams(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	cached, err := a.cache.Get(ctx, "exams:all").Bytes()
	if err == nil {
		w.Header().Set("Content-Type", "application/json")
		w.Write(cached)
		return
	}
	if err != redis.Nil {
		log.Println(err)
	}

	rows, err := a.db.Query(ctx,
		`SELECT id, title, teacher_id, duration_minutes
		 FROM exams
		 ORDER BY id`)
	if err != nil {
		log.Println(err)
		http.Error(w, "could not load exams", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	exams := []examListItem{}
	for rows.Next() {
		var exam examListItem
		if err := rows.Scan(&exam.ID, &exam.Title, &exam.TeacherID, &exam.DurationMinutes); err != nil {
			log.Println(err)
			http.Error(w, "could not load exams", http.StatusInternalServerError)
			return
		}
		exams = append(exams, exam)
	}
	if err := rows.Err(); err != nil {
		log.Println(err)
		http.Error(w, "could not load exams", http.StatusInternalServerError)
		return
	}

	body, err := json.Marshal(exams)
	if err != nil {
		log.Println(err)
		http.Error(w, "could not load exams", http.StatusInternalServerError)
		return
	}
	if err := a.cache.Set(ctx, "exams:all", body, 0).Err(); err != nil {
		log.Println(err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(body)
}

func (a *app) createExam(w http.ResponseWriter, r *http.Request) {
	var req examRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if err := validateExam(req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	tx, err := a.db.Begin(ctx)
	if err != nil {
		log.Println(err)
		http.Error(w, "could not save exam", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback(ctx)

	title := strings.TrimSpace(req.Title)
	var examID int64
	err = tx.QueryRow(ctx,
		`INSERT INTO exams (title, teacher_id, duration_minutes)
		 VALUES ($1, $2, $3)
		 RETURNING id`,
		title, req.TeacherID, req.DurationMinutes,
	).Scan(&examID)
	if err != nil {
		log.Println(err)
		http.Error(w, "could not save exam", http.StatusInternalServerError)
		return
	}

	for _, question := range req.Questions {
		var questionID int64
		err = tx.QueryRow(ctx,
			`INSERT INTO questions (exam_id, body, correct_option)
			 VALUES ($1, $2, $3)
			 RETURNING id`,
			examID, strings.TrimSpace(question.Body), question.CorrectOption,
		).Scan(&questionID)
		if err != nil {
			log.Println(err)
			http.Error(w, "could not save exam", http.StatusInternalServerError)
			return
		}

		for number, option := range question.Options {
			_, err = tx.Exec(ctx,
				`INSERT INTO options (question_id, option_number, body)
				 VALUES ($1, $2, $3)`,
				questionID, number+1, strings.TrimSpace(option),
			)
			if err != nil {
				log.Println(err)
				http.Error(w, "could not save exam", http.StatusInternalServerError)
				return
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		log.Println(err)
		http.Error(w, "could not save exam", http.StatusInternalServerError)
		return
	}

	// The saved list no longer matches the database, so the next read rebuilds it.
	if err := a.cache.Del(ctx, "exams:all").Err(); err != nil {
		log.Println(err)
	}
	a.publish(ctx, "exam.created", map[string]any{
		"exam_id":    examID,
		"title":      title,
		"teacher_id": req.TeacherID,
	})

	writeJSON(w, http.StatusCreated, examResponse{
		ID:            examID,
		Title:         title,
		QuestionCount: len(req.Questions),
	})
}

func validateExam(req examRequest) error {
	if strings.TrimSpace(req.Title) == "" {
		return errors.New("title is required")
	}
	if req.TeacherID <= 0 {
		return errors.New("teacher_id must be a positive number")
	}
	if req.DurationMinutes <= 0 {
		return errors.New("duration_minutes must be greater than 0")
	}
	if len(req.Questions) == 0 {
		return errors.New("exam needs at least one question")
	}

	for _, question := range req.Questions {
		if strings.TrimSpace(question.Body) == "" {
			return errors.New("question text is required")
		}
		if question.CorrectOption < 1 || question.CorrectOption > 4 {
			return errors.New("correct_option must be 1, 2, 3, or 4")
		}
		if len(question.Options) != 4 {
			return errors.New("each question needs exactly 4 options")
		}
		for _, option := range question.Options {
			if strings.TrimSpace(option) == "" {
				return errors.New("option text is required")
			}
		}
	}
	return nil
}

func writeJSON(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(value)
}
