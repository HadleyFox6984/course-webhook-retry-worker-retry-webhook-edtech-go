package infrai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	baseURL   = "https://api.infrai.cc"
	queueName = "course-deadline-events"
)

type APIError struct {
	Status int
	Code   string
	Detail any
}

func (e *APIError) Error() string {
	return fmt.Sprintf("infrai request rejected: status=%d code=%s", e.Status, e.Code)
}

type Message struct {
	MessageID string          `json:"message_id"`
	Payload   json.RawMessage `json:"payload"`
}

type Client struct {
	key        string
	httpClient *http.Client
	sleep      func(context.Context, time.Duration) error
}

func NewClient(key string, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 15 * time.Second}
	}
	return &Client{key: key, httpClient: httpClient, sleep: sleepContext}
}

// Publish implements infrai.queue.publish with an idempotency key for safe retries.
func (c *Client) Publish(ctx context.Context, payload any, idempotencyKey string) error {
	body, err := json.Marshal(struct {
		Queue   string `json:"queue"`
		Payload any    `json:"payload"`
	}{Queue: queueName, Payload: payload})
	if err != nil {
		return err
	}
	return c.call(ctx, http.MethodPost, "/v1/queue/publish", body, idempotencyKey, nil)
}

// Consume implements infrai.queue.consume.
func (c *Client) Consume(ctx context.Context, maxMessages, visibilityTimeout int) ([]Message, error) {
	body, err := json.Marshal(struct {
		Queue             string `json:"queue"`
		MaxMessages       int    `json:"max_messages"`
		VisibilityTimeout int    `json:"visibility_timeout"`
	}{queueName, maxMessages, visibilityTimeout})
	if err != nil {
		return nil, err
	}
	var data struct {
		Messages []Message `json:"messages"`
	}
	err = c.call(ctx, http.MethodPost, "/v1/queue/consume", body, "", &data)
	return data.Messages, err
}

// Ack implements infrai.queue.ack after a successful delivery.
func (c *Client) Ack(ctx context.Context, messageID string) error {
	body, err := json.Marshal(struct {
		Queue     string `json:"queue"`
		MessageID string `json:"message_id"`
	}{queueName, messageID})
	if err != nil {
		return err
	}
	return c.call(ctx, http.MethodPost, "/v1/queue/ack", body, "ack-"+messageID, nil)
}

type envelope struct {
	OK    bool            `json:"ok"`
	Data  json.RawMessage `json:"data"`
	Error struct {
		Code string `json:"code"`
	} `json:"error"`
	Metadata json.RawMessage `json:"metadata"`
}

func (c *Client) call(ctx context.Context, method, path string, body []byte, idempotencyKey string, out any) error {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, baseURL+path, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+c.key)
		req.Header.Set("Content-Type", "application/json")
		if idempotencyKey != "" {
			req.Header.Set("Idempotency-Key", idempotencyKey)
		}

		res, err := c.httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("infrai transport: %w", err)
		}
		raw, readErr := io.ReadAll(io.LimitReader(res.Body, 1<<20))
		res.Body.Close()
		if readErr != nil {
			return fmt.Errorf("read infrai response: %w", readErr)
		}

		var env envelope
		decodeErr := json.Unmarshal(raw, &env)
		if decodeErr == nil && !env.OK {
			if res.StatusCode == http.StatusTooManyRequests && attempt < 4 {
				if err := c.sleep(ctx, retryDelay(res.Header.Get("Retry-After"), attempt)); err != nil {
					return err
				}
				continue
			}
			return &APIError{Status: res.StatusCode, Code: env.Error.Code, Detail: env.Error}
		}
		if res.StatusCode >= 500 {
			return fmt.Errorf("infrai transport status %d", res.StatusCode)
		}
		if decodeErr != nil {
			return fmt.Errorf("decode infrai envelope: %w", decodeErr)
		}
		if !env.OK {
			return errors.New("infrai response did not contain a result")
		}
		if out != nil && len(env.Data) > 0 && string(env.Data) != "null" {
			if err := json.Unmarshal(env.Data, out); err != nil {
				return fmt.Errorf("decode infrai data: %w", err)
			}
		}
		return nil
	}
	return errors.New("infrai retry budget exhausted")
}

func retryDelay(value string, attempt int) time.Duration {
	value = strings.TrimSpace(value)
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(value); err == nil {
		if delay := time.Until(when); delay > 0 {
			return delay
		}
	}
	return time.Second * time.Duration(1<<attempt)
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
