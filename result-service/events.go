package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func (a *app) publish(ctx context.Context, topic string, value any) {
	if a.events == nil {
		return
	}
	body, err := json.Marshal(value)
	if err != nil {
		log.Println(err)
		return
	}
	if err := a.events.WriteMessages(ctx, kafka.Message{Topic: topic, Value: body}); err != nil {
		log.Println(err)
	}
}

func (a *app) listen(ctx context.Context, topic string, handle func([]byte)) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{"localhost:9092"},
		GroupID:     "result-" + topic,
		Topic:       topic,
		StartOffset: kafka.FirstOffset,
		MaxBytes:    1e6,
	})
	defer reader.Close()

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Println(topic, err)
			return
		}
		handle(msg.Value)
	}
}

func (a *app) saveExamRef(payload []byte) {
	var body struct {
		ExamID    int64  `json:"exam_id"`
		Title     string `json:"title"`
		TeacherID int64  `json:"teacher_id"`
	}
	if err := json.Unmarshal(payload, &body); err != nil || body.ExamID <= 0 || body.Title == "" || body.TeacherID <= 0 {
		log.Println("bad exam.created message")
		return
	}

	_, err := a.db.Exec(context.Background(),
		`INSERT INTO exam_refs (exam_id, title, teacher_id)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (exam_id) DO UPDATE
		 SET title = EXCLUDED.title, teacher_id = EXCLUDED.teacher_id`,
		body.ExamID, body.Title, body.TeacherID,
	)
	if err != nil {
		log.Println(err)
	}
}

func (a *app) saveSubmission(payload []byte) {
	var body struct {
		AttemptID   int64     `json:"attempt_id"`
		ExamID      int64     `json:"exam_id"`
		StudentID   int64     `json:"student_id"`
		SubmittedAt time.Time `json:"submitted_at"`
	}
	if err := json.Unmarshal(payload, &body); err != nil || body.AttemptID <= 0 || body.ExamID <= 0 || body.StudentID <= 0 || body.SubmittedAt.IsZero() {
		log.Println("bad exam.submitted message")
		return
	}

	_, err := a.db.Exec(context.Background(),
		`INSERT INTO submission_log (attempt_id, exam_id, student_id, submitted_at)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (attempt_id) DO NOTHING`,
		body.AttemptID, body.ExamID, body.StudentID, body.SubmittedAt,
	)
	if err != nil {
		log.Println(err)
	}
}
