package main

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// schema.sql is packed into the program, so it runs no matter which folder we start from.
//
//go:embed schema.sql
var schemaFile embed.FS

func main() {
	ctx := context.Background()

	db, err := pgxpool.New(ctx, "postgres://admin:admin@localhost:5432/exam_db?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := db.Ping(ctx); err != nil {
		log.Fatal(err)
	}

	if err := createTables(ctx, db); err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		if err := db.Ping(r.Context()); err != nil {
			http.Error(w, "database is down", http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("POST /exams", createExam(db))

	log.Println("exam-service is listening on http://localhost:8081")
	log.Fatal(http.ListenAndServe(":8081", mux))
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

func createExam(db *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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
		tx, err := db.Begin(ctx)
		if err != nil {
			log.Println(err)
			http.Error(w, "could not save exam", http.StatusInternalServerError)
			return
		}
		defer tx.Rollback(ctx)

		var examID int64
		err = tx.QueryRow(ctx,
			`INSERT INTO exams (title, teacher_id, duration_minutes)
			 VALUES ($1, $2, $3)
			 RETURNING id`,
			strings.TrimSpace(req.Title), req.TeacherID, req.DurationMinutes,
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

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(examResponse{
			ID:            examID,
			Title:         strings.TrimSpace(req.Title),
			QuestionCount: len(req.Questions),
		})
	}
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
