# VitaLoom Data Producer

A standalone Go CLI that generates synthetic Heart Rate and Steps events and publishes them through the configured VitaLoom Nginx ingress. It is intended for local demos, integration checks and controlled development workloads — not as a source of real wearable data or medical advice.

## Current capabilities

The initial implementation has been migrated from the end-to-end demo:

- Generates batches of synthetic Heart Rate observations and Step increments.
- Sends them to the matching Backend batch endpoints over HTTP.
- Uses stable event IDs and preserves the same request body during retries.
- Supports a configurable batch size, interval, request timeout, bounded retries and optional run duration.
- Logs per-batch results and a run summary.
- Includes unit tests for configuration, event generation, HTTP behavior and producer lifecycle.
- Provisions deterministic test accounts through Nginx and keeps authentication sessions in memory for callers.

The following are future work, not implemented capabilities: profile-driven behavioral scenarios, UI-controlled simulation, load-test profiles, and richer observability.

## Automated tests

Run this from the repository root. The tests use a local fake API.

```powershell
go test ./...
```

## Architecture

```text
VitaLoom Data Producer (Go)
        |
        | HTTP API requests
        v
   Nginx ingress
        |
        v
VitaLoom Backend (Spring Boot)
        |
        v
     MongoDB
```

The Producer never connects to MongoDB. Backend validation, persistence, deduplication and aggregation remain authoritative.

## API contract

The canonical contract lives in [vitaloom-backend/docs/api/api-contract.md](https://github.com/vitaloom/vitaloom-backend/blob/main/docs/api/api-contract.md).

| Method | Endpoint | Event type |
|---|---|---|
| POST | `/api/heart-rate/batch` | Heart Rate observations |
| POST | `/api/steps/batch` | Step increments |

Do not change payload fields, response handling, event identity or timestamp semantics independently of the Backend contract.

## Prerequisites

- Go 1.23 or newer.
- A running VitaLoom test environment with Nginx routing to the corresponding Backend endpoints.
- An explicitly authorized test Nginx URL reachable from the Producer process.

## Run locally

From the repository root, copy `.env.example` to `.env`, then set `VITALOOM_API_BASE_URL` to your authorized test Nginx URL and `VITALOOM_TEST_PASSWORD` to a password of at least eight characters. The CLI reads `.env` from its current directory; process environment variables take precedence. Values use simple `NAME=value` lines, without quoting or expansion. Provision accounts `1`–`1000` as needed; existing accounts keep their current password.

```powershell
Copy-Item .env.example .env
```

After editing `.env`, provision the accounts you need (`go run . provision` creates all 1,000):

```powershell
go run . provision --from 1 --to 10
```

Run both metrics together, or use `step` or `heart-rate` for a single metric. Steps are generated every second; their batch interval controls flushes. Heart Rate without scheduling flags submits one batch and reads the summary; scheduling flags enable its scheduler. The multi-user example requires ten provisioned accounts.

```powershell
go run . --account 1 --duration 30s
go run . step --account 1 --duration 30s
go run . heart-rate --account 1 --measurement-interval 1s --batch-interval 5s --duration 30s
go run . heart-rate --virtual-users 10 --ramp-up 5s --duration 20s --measurement-interval 1s --batch-interval 5s
```

Options and defaults:

| Scope | Option | Default | Description |
|---|---|---:|---|
| Combined | `--account` | `1` | Provisioned account (1–1000) |
| Combined | `--api-base-url` | required | Authorized test Nginx URL |
| Combined, `step` | `--batch-size` | `5` | Events per batch (1–500) |
| Combined | `--interval` | `5s` | Flush interval for both metrics |
| Combined, `step` | `--duration` | `0s` | Run duration; `0s` runs until stopped |
| `step` | `--batch-interval` | `5s` | Steps flush interval; uses `VITALOOM_BATCH_INTERVAL` or falls back to `VITALOOM_INTERVAL` |
| `heart-rate` | `--measurement-interval` | `1s` | Scheduled measurement interval |
| `heart-rate` | `--batch-interval` | `5s` | Scheduled batch flush interval |
| `heart-rate` | `--virtual-users` | required | Multi-user account count (1–1000) |
| `heart-rate` | `--ramp-up` | `0s` | Multi-user ramp; must be less than the positive duration |
| `heart-rate` | `--devices-per-user` | `1` | Only one device per user is supported |

`--interval` applies to both streams in combined mode; single-metric commands use `--batch-interval`. Combined mode and `step` accept `VITALOOM_BATCH_SIZE` and `VITALOOM_DURATION`; combined `--interval` uses `VITALOOM_INTERVAL`, while Steps uses `VITALOOM_BATCH_INTERVAL` (falling back to `VITALOOM_INTERVAL`). Set `VITALOOM_API_BASE_URL` for `step`; combined mode needs it unless overridden by `--api-base-url`. Both require `VITALOOM_TEST_PASSWORD`. Transport settings are environment-only: `VITALOOM_TIMEOUT` defaults to `5s`, `VITALOOM_MAX_RETRIES` to `3` retries (0–10), and `VITALOOM_RETRY_DELAY` to `500ms`.

## Development checks

```bash
gofmt -w *.go
go test ./...
go vet ./...
go build ./...
```

## Repository layout

```text
.
├── main.go
├── config.go
├── events.go
├── client.go
├── producer.go
├── *_test.go
├── go.mod
├── AGENTS.md
└── README.md
```

## Scope boundaries

This repository implements a development simulator. It does not read Apple Health/HealthKit or connect to a physical wearable. Sleep and Water remain manual-entry metrics owned by the Mobile app and Backend. Keep the first deployment simple: one Producer process and one Spring Boot Backend instance.
