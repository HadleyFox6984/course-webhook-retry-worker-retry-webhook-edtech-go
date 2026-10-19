package delivery

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"
)

type fakeQueue struct {
	messages []QueueMessage
	acked    []string
}

func (q *fakeQueue) Consume(context.Context, int, int) ([]QueueMessage, error) {
	return q.messages, nil
}

func (q *fakeQueue) Ack(_ context.Context, messageID string) error {
	q.acked = append(q.acked, messageID)
	return nil
}

func TestRunOnceAcknowledgesOnlyAcceptedEducatorReports(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		wantAcked  []string
		wantHeader string
	}{
		{name: "accepted report leaves the retry queue", status: http.StatusNoContent, wantAcked: []string{"msg-7"}, wantHeader: "evt-7"},
		{name: "rejected report remains available for retry", status: http.StatusConflict, wantAcked: nil, wantHeader: "evt-7"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var eventHeader string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				eventHeader = r.Header.Get("X-Event-ID")
				w.WriteHeader(tt.status)
			}))
			defer server.Close()

			event := CourseEvent{
				EventID: "evt-7", CourseID: "go-101", LearnerID: "learner-42",
				Deadline:  time.Date(2026, 9, 8, 17, 0, 0, 0, time.UTC),
				ReportURL: server.URL, DeliveryKind: "deadline.reminder",
			}
			payload, err := json.Marshal(event)
			if err != nil {
				t.Fatal(err)
			}
			queue := &fakeQueue{messages: []QueueMessage{{MessageID: "msg-7", Payload: payload}}}
			sender := NewSender(queue, server.Client())

			if _, err := sender.RunOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(queue.acked, tt.wantAcked) {
				t.Fatalf("acked messages = %v, want %v", queue.acked, tt.wantAcked)
			}
			if eventHeader != tt.wantHeader {
				t.Fatalf("X-Event-ID = %q, want %q", eventHeader, tt.wantHeader)
			}
		})
	}
}
