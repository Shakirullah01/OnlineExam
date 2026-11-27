package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Shakirullah01/OnlineExam/result-service/resultpb"
)

type answerInput struct {
	QuestionID     int64 `json:"question_id"`
	SelectedOption int   `json:"selected_option"`
}

type optionView struct {
	Number int    `json:"number"`
	Body   string `json:"body"`
}

type questionView struct {
	ID      int64        `json:"id"`
	Body    string       `json:"body"`
	Options []optionView `json:"options"`
}

func (a *app) startAttempt(w http.ResponseWriter, r *http.Request) {
	examID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || examID <= 0 {
		http.Error(w, "exam id must be a positive number", http.StatusBadRequest)
		return
	}

	var req struct {
		StudentID int64 `json:"student_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if req.StudentID <= 0 {
		http.Error(w, "student_id must be a positive number", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	var title string
	err = a.db.QueryRow(ctx, `SELECT title FROM exams WHERE id = $1`, examID).Scan(&title)
	if errors.Is(err, pgx.ErrNoRows) {
		http.Error(w, "exam not found", http.StatusNotFound)
		return
	}
	if err != nil {
		log.Println(err)
		http.Error(w, "could not start exam", http.StatusInternalServerError)
		return
	}

	var attemptID int64
	var status string
	err = a.db.QueryRow(ctx,
		`SELECT id, status FROM attempts WHERE exam_id = $1 AND student_id = $2`,
		examID, req.StudentID,
	).Scan(&attemptID, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		err = a.db.QueryRow(ctx,
			`INSERT INTO attempts (exam_id, student_id, status)
			 VALUES ($1, $2, 'started')
			 RETURNING id`,
			examID, req.StudentID,
		).Scan(&attemptID)
		if err != nil {
			err = a.db.QueryRow(ctx,
				`SELECT id, status FROM attempts WHERE exam_id = $1 AND student_id = $2`,
				examID, req.StudentID,
			).Scan(&attemptID, &status)
		} else {
			status = "started"
		}
	}
	if err != nil {
		log.Println(err)
		http.Error(w, "could not start exam", http.StatusInternalServerError)
		return
	}
	if status != "started" {
		http.Error(w, "this student already took this exam", http.StatusConflict)
		return
	}

	questions, err := a.loadQuestions(ctx, examID)
	if err != nil {
		log.Println(err)
		http.Error(w, "could not start exam", http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"attempt_id": attemptID,
		"exam_id":    examID,
		"title":      title,
		"questions":  questions,
	})
}

func (a *app) loadQuestions(ctx context.Context, examID int64) ([]questionView, error) {
	rows, err := a.db.Query(ctx,
		`SELECT q.id, q.body, o.option_number, o.body
		 FROM questions q
		 JOIN options o ON o.question_id = q.id
		 WHERE q.exam_id = $1
		 ORDER BY q.id, o.option_number`,
		examID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	questions := []questionView{}
	for rows.Next() {
		var id int64
		var body, optionBody string
		var number int
		if err := rows.Scan(&id, &body, &number, &optionBody); err != nil {
			return nil, err
		}
		if len(questions) == 0 || questions[len(questions)-1].ID != id {
			questions = append(questions, questionView{ID: id, Body: body})
		}
		last := len(questions) - 1
		questions[last].Options = append(questions[last].Options, optionView{Number: number, Body: optionBody})
	}
	return questions, rows.Err()
}

func (a *app) submitAttempt(w http.ResponseWriter, r *http.Request) {
	attemptID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || attemptID <= 0 {
		http.Error(w, "attempt id must be a positive number", http.StatusBadRequest)
		return
	}

	var req struct {
		Answers []answerInput `json:"answers"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	var examID, studentID int64
	var status string
	err = a.db.QueryRow(ctx,
		`SELECT exam_id, student_id, status FROM attempts WHERE id = $1`,
		attemptID,
	).Scan(&examID, &studentID, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		http.Error(w, "attempt not found", http.StatusNotFound)
		return
	}
	if err != nil {
		log.Println(err)
		http.Error(w, "could not submit exam", http.StatusInternalServerError)
		return
	}
	if status != "started" {
		http.Error(w, "this exam was already submitted", http.StatusConflict)
		return
	}

	questionIDs, answerKey, err := a.loadAnswerKey(ctx, examID)
	if err != nil {
		log.Println(err)
		http.Error(w, "could not submit exam", http.StatusInternalServerError)
		return
	}
	if err := validateAnswers(questionIDs, req.Answers); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	tx, err := a.db.Begin(ctx)
	if err != nil {
		log.Println(err)
		http.Error(w, "could not submit exam", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback(ctx)

	for _, answer := range req.Answers {
		_, err = tx.Exec(ctx,
			`INSERT INTO attempt_answers (attempt_id, question_id, selected_option)
			 VALUES ($1, $2, $3)`,
			attemptID, answer.QuestionID, answer.SelectedOption,
		)
		if err != nil {
			log.Println(err)
			http.Error(w, "could not submit exam", http.StatusInternalServerError)
			return
		}
	}

	tag, err := tx.Exec(ctx,
		`UPDATE attempts
		 SET status = 'submitted', submitted_at = now()
		 WHERE id = $1 AND status = 'started'`,
		attemptID,
	)
	if err != nil {
		log.Println(err)
		http.Error(w, "could not submit exam", http.StatusInternalServerError)
		return
	}
	if tag.RowsAffected() != 1 {
		http.Error(w, "this exam was already submitted", http.StatusConflict)
		return
	}

	grpcAnswers := make([]*resultpb.Answer, 0, len(req.Answers))
	for _, answer := range req.Answers {
		grpcAnswers = append(grpcAnswers, &resultpb.Answer{
			SelectedOption: int32(answer.SelectedOption),
			CorrectOption:  int32(answerKey[answer.QuestionID]),
		})
	}
	graded, err := a.grader.CalculateResult(ctx, &resultpb.CalculateRequest{
		AttemptId: attemptID,
		ExamId:    examID,
		StudentId: studentID,
		Answers:   grpcAnswers,
	})
	if err != nil {
		log.Println(err)
		http.Error(w, "could not grade the exam", http.StatusBadGateway)
		return
	}

	if err := tx.Commit(ctx); err != nil {
		log.Println(err)
		http.Error(w, "could not submit exam", http.StatusInternalServerError)
		return
	}

	a.publish(ctx, "exam.submitted", map[string]any{
		"attempt_id":   attemptID,
		"exam_id":      examID,
		"student_id":   studentID,
		"submitted_at": time.Now().UTC().Format(time.RFC3339),
	})

	writeJSON(w, http.StatusOK, map[string]any{
		"attempt_id":      attemptID,
		"correct_count":   graded.GetCorrectCount(),
		"total_questions": graded.GetTotalQuestions(),
		"percentage":      graded.GetPercentage(),
	})
}

func (a *app) loadAnswerKey(ctx context.Context, examID int64) ([]int64, map[int64]int, error) {
	rows, err := a.db.Query(ctx,
		`SELECT id, correct_option FROM questions WHERE exam_id = $1 ORDER BY id`,
		examID,
	)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	ids := []int64{}
	key := map[int64]int{}
	for rows.Next() {
		var id int64
		var correct int
		if err := rows.Scan(&id, &correct); err != nil {
			return nil, nil, err
		}
		ids = append(ids, id)
		key[id] = correct
	}
	return ids, key, rows.Err()
}

func validateAnswers(questionIDs []int64, answers []answerInput) error {
	if len(answers) != len(questionIDs) {
		return errors.New("answer every question once")
	}
	allowed := map[int64]bool{}
	for _, id := range questionIDs {
		allowed[id] = true
	}
	seen := map[int64]bool{}
	for _, answer := range answers {
		if !allowed[answer.QuestionID] {
			return errors.New("question is not part of this exam")
		}
		if seen[answer.QuestionID] {
			return errors.New("answer every question once")
		}
		seen[answer.QuestionID] = true
		if answer.SelectedOption < 1 || answer.SelectedOption > 4 {
			return errors.New("selected_option must be 1, 2, 3, or 4")
		}
	}
	return nil
}
