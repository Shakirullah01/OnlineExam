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

func (a *app) listenForGrades(ctx context.Context) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{"localhost:9092"},
		GroupID:     "exam-service",
		Topic:       "result.calculated",
		StartOffset: kafka.FirstOffset,
		MaxBytes:    1e6,
	})
	defer reader.Close()

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Println(err)
			return
		}
		a.markGraded(ctx, msg.Value)
	}
}

// markGraded tries more than once. The Kafka message can arrive before submit has committed.
func (a *app) markGraded(ctx context.Context, payload []byte) {
	var body struct {
		AttemptID int64 `json:"attempt_id"`
	}
	if err := json.Unmarshal(payload, &body); err != nil || body.AttemptID <= 0 {
		log.Println("bad result.calculated message")
		return
	}

	for i := 0; i < 10; i++ {
		tag, err := a.db.Exec(ctx,
			`UPDATE attempts SET status = 'graded' WHERE id = $1 AND status = 'submitted'`,
			body.AttemptID,
		)
		if err != nil {
			log.Println(err)
			return
		}
		if tag.RowsAffected() == 1 {
			return
		}

		var status string
		err = a.db.QueryRow(ctx, `SELECT status FROM attempts WHERE id = $1`, body.AttemptID).Scan(&status)
		if err == nil && status == "graded" {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}
