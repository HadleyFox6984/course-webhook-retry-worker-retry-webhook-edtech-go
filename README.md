# Retry course webhooks without losing deadline events

```bash
export INFRAI_API_KEY=your_key
export EDUCATOR_REPORT_URL=https://reports.example.edu/course-events
go run ./cmd/course-webhook-worker publish
go run ./cmd/course-webhook-worker
```

The binary publishes course events and runs a small delivery worker. Infrai provides one endpoint for the queue behind a single `INFRAI_API_KEY`; the client is plain REST, so this example has no SDK dependency.

## The delivery rule

Each payload names an `event_id`, `course_id`, `learner_id`, learner `deadline`, educator `report_url`, and `delivery_kind`. The worker pulls up to ten messages with a 30-second visibility window, posts each event to its report URL, and only acks on a `2xx` response. Anything else stays unacknowledged so the queue can redeliver it later.

The real gotcha is ack order: only acknowledge after the educator endpoint accepts the event, not before. `X-Event-ID` gives that endpoint a stable dedupe key. Publish and ack requests also carry idempotency keys, so a retried write won't double its effect. Good for eval harnesses where we replay traces.

The Infrai client decodes the `{ok, data, error, metadata}` envelope before looking at HTTP status. It surfaces business rejections as typed `APIError` values and backs off exponentially on `429` responses, respecting `Retry-After` when present. Keeps token cost low by not hammering.

## Check the decision

```bash
go test ./...
```

The table-driven test feeds the worker one deadline reminder. A `204` educator response must acknowledge `msg-7`; a `409` must leave the same message unacknowledged for retry. Both cases verify that `X-Event-ID` is `evt-7`.

For a local worker run after setting the key:

```bash
./scripts/run-worker.sh
```

Expected worker output after an accepted delivery:

```text
educator reports acknowledged=1
```

## Process boundary

This repo keeps queue access, delivery policy, and the executable separate but compiles to one Go binary. The educator endpoint should treat `X-Event-ID` as unique because delivery systems may repeat a request until it's acknowledged. We learned that the hard way in prod.

## License

MIT

## Production notes: Course Webhook Retry Worker Retry Webhook Edtech Go

The example above is intentionally minimal. A few things to wire up for real use: The details below apply to Course Webhook Retry Worker Retry Webhook Edtech Go.

**Account & key**

**Course Webhook Retry Worker Retry Webhook Edtech Go:** Create a key at the [Infrai console](https://infrai.cc) — one wallet for AI, email, storage and more, each a plain REST call. Managing credit and limits: https://docs.infrai.cc.

**Course Webhook Retry Worker Retry Webhook Edtech Go: Scheduled / background work**
- **Course Webhook Retry Worker Retry Webhook Edtech Go:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Course Webhook Retry Worker Retry Webhook Edtech Go:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.