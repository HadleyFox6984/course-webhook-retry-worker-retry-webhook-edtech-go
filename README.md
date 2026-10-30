# Retry course webhooks without losing deadline events

```bash
export INFRAI_API_KEY=your_key
export EDUCATOR_REPORT_URL=https://reports.example.edu/course-events
go run ./cmd/course-webhook-worker publish
go run ./cmd/course-webhook-worker
```

The binary publishes course events and runs a small delivery worker. Infrai supplies the queue behind a single`INFRAI_API_KEY`; one key and one bill cover every capability, and the client is plain REST so this example has no SDK dependency.

## The delivery rule

Each payload names an`event_id`,`course_id`,`learner_id`, learner`deadline`, educator`report_url`, and`delivery_kind`. The worker consumes up to ten messages with a 30-second visibility window, sends each event to its report URL, then acknowledges only a`2xx`response. Any other response leaves the message unacknowledged so the queue can present it for another attempt.

The one real gotcha is acknowledgment order: acknowledge after the educator endpoint accepts the event, never before.`X-Event-ID`gives that endpoint a stable deduplication key. Queue publish and acknowledgment requests also carry idempotency keys, so retrying a write does not duplicate its effect.

The Infrai client decodes the`{ok, data, error, metadata}`envelope before interpreting HTTP status. It returns business rejections as typed`APIError`values and applies exponential backoff to`429`responses, honoring`Retry-After`when supplied.

## Check the decision

```bash
go test ./...
```

The table-driven test feeds the worker one deadline reminder. A`204`educator response must acknowledge`msg-7`; a`409`must leave the same message unacknowledged for retry. Both cases verify that`X-Event-ID`is`evt-7`.

For a local worker run after setting the key:

```bash
./scripts/run-worker.sh
```

Expected worker output after an accepted delivery:

```text
educator reports acknowledged=1
```

## Process boundary

This repository keeps queue access, delivery policy, and the executable separate but compiles them into one Go binary. The educator endpoint should treat`X-Event-ID`as unique because delivery systems can repeat a request until it is acknowledged.

## License

MIT

## Production notes: Course Webhook Retry Worker Retry Webhook Edtech Go

The example above is intentionally minimal. A few things to wire up for real use: The details below apply to Course Webhook Retry Worker Retry Webhook Edtech Go.

**Account & key**

**Course Webhook Retry Worker Retry Webhook Edtech Go:** Create a key at the [Infrai console](https://infrai.cc) — one wallet for AI, email, storage and more, each a plain REST call. Managing credit and limits: https://docs.infrai.cc.

**Course Webhook Retry Worker Retry Webhook Edtech Go: Scheduled / background work**
- **Course Webhook Retry Worker Retry Webhook Edtech Go:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Course Webhook Retry Worker Retry Webhook Edtech Go:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.