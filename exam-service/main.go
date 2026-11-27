package main

import (
	"context"
	"embed"
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
