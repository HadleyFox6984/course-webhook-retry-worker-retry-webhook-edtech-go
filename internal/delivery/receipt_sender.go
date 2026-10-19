package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type CourseEvent struct {
	EventID      string    `json:"event_id"`
	CourseID     string    `json:"course_id"`
	LearnerID    string    `json:"learner_id"`
	Deadline     time.Time `json:"deadline"`
	ReportURL    string    `json:"report_url"`
	DeliveryKind string    `json:"delivery_kind"`
}

type Queue interface {
	Consume(context.Context, int, int) ([]QueueMessage, error)
	Ack(context.Context, string) error
}

type QueueMessage struct {
	MessageID string
	Payload   json.RawMessage
}

type Sender struct {
	queue      Queue
	httpClient *http.Client
}

func NewSender(queue Queue, httpClient *http.Client) *Sender {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	return &Sender{queue: queue, httpClient: httpClient}
}

// RunOnce reports each valid course event and acknowledges only accepted deliveries.
func (s *Sender) RunOnce(ctx context.Context) (int, error) {
	messages, err := s.queue.Consume(ctx, 10, 30)
	if err != nil {
		return 0, err
	}
	acknowledged := 0
	for _, message := range messages {
		var event CourseEvent
		if err := json.Unmarshal(message.Payload, &event); err != nil {
			continue
		}
		accepted, err := s.deliver(ctx, event)
		if err != nil || !accepted {
			continue
		}
		if err := s.queue.Ack(ctx, message.MessageID); err != nil {
			return acknowledged, err
		}
		acknowledged++
	}
	return acknowledged, nil
}

func (s *Sender) deliver(ctx context.Context, event CourseEvent) (bool, error) {
	body, err := json.Marshal(event)
	if err != nil {
		return false, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, event.ReportURL, bytes.NewReader(body))
	if err != nil {
		return false, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Event-ID", event.EventID)
	res, err := s.httpClient.Do(req)
	if err != nil {
		return false, fmt.Errorf("deliver course event: %w", err)
	}
	defer res.Body.Close()
	return res.StatusCode >= 200 && res.StatusCode < 300, nil
}
