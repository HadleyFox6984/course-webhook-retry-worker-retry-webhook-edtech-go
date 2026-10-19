#!/bin/sh
set -eu

go test ./...
exec go run ./cmd/course-webhook-worker
