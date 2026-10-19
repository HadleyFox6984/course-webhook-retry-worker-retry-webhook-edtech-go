package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/example/course-webhook-worker/internal/delivery"
	"github.com/example/course-webhook-worker/internal/infrai"
)

type deliveryQueue struct{ client *infrai.Client }

func (q deliveryQueue) Consume(ctx context.Context, maxMessages, visibilityTimeout int) ([]delivery.QueueMessage, error) {
	messages, err := q.client.Consume(ctx, maxMessages, visibilityTimeout)
	if err != nil {
		return nil, err
	}
	result := make([]delivery.QueueMessage, len(messages))
	for i, message := range messages {
		result[i] = delivery.QueueMessage{MessageID: message.MessageID, Payload: message.Payload}
	}
	return result, nil
}

func (q deliveryQueue) Ack(ctx context.Context, messageID string) error {
	return q.client.Ack(ctx, messageID)
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		log.Fatal("INFRAI_API_KEY is required")
	}
	client := infrai.NewClient(key, nil)
	ctx := context.Background()

	if len(os.Args) > 1 && os.Args[1] == "publish" {
		publishExample(ctx, client)
		return
	}

	sender := delivery.NewSender(deliveryQueue{client: client}, nil)
	for {
		count, err := sender.RunOnce(ctx)
		if err != nil {
			log.Printf("worker cycle: %v", err)
		} else {
			log.Printf("educator reports acknowledged=%d", count)
		}
		time.Sleep(2 * time.Second)
	}
}

func publishExample(ctx context.Context, client *infrai.Client) {
	reportURL := os.Getenv("EDUCATOR_REPORT_URL")
	if reportURL == "" {
		log.Fatal("EDUCATOR_REPORT_URL is required for publish")
	}
	event := delivery.CourseEvent{
		EventID:      "deadline-go-101-learner-42-20260908",
		CourseID:     "go-101",
		LearnerID:    "learner-42",
		Deadline:     time.Date(2026, 9, 8, 17, 0, 0, 0, time.UTC),
		ReportURL:    reportURL,
		DeliveryKind: "deadline.reminder",
	}
	if err := client.Publish(ctx, event, event.EventID); err != nil {
		log.Fatal(err)
	}
	encoded, _ := json.Marshal(event)
	fmt.Printf("published %s\n", encoded)
}
