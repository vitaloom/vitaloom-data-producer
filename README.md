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

From the repository root, copy `.env.example` to `.env`, set `VITALOOM_TEST_PASSWORD` to the password for your authorized test accounts, and adjust `VITALOOM_API_BASE_URL` if needed. The CLI reads `.env` from its current directory; process environment variables take precedence. Values use simple `NAME=value` lines, with no quoting or expansion.

```powershell
Copy-Item .env.example .env
```

Provision or verify one test account, then run the one-shot Heart Rate command:

```powershell
go run . provision --from 1 --to 1
go run . heart-rate --account 1
```

The Heart Rate command logs in as `loadtest-0001@vitaloom.test`, submits one Heart Rate batch, and reads that account's summary through the configured Nginx URL. Provision the account first if needed.

To schedule one measurement per device independently from batch flushes, use positive intervals and an optional duration. The duration starts after login; `0s` runs until Ctrl+C. Scheduled Heart Rate runs keep at most 1,000 outstanding events, send batches of up to 500, and drain pending events for up to five seconds on shutdown.

```powershell
go run . heart-rate --account 1 --measurement-interval 1s --batch-interval 5s --duration 30s
```

Scheduled mode defaults to a `1s` measurement interval and `5s` batch interval. Supplying none of the scheduling flags keeps the one-shot command above.

Use `--virtual-users` to select how many provisioned accounts to run. The Producer logs in to those accounts one at a time before the timer starts. Each uses its own token and device. The duration includes ramp-up, and `--devices-per-user` accepts `1`.

```powershell
go run . heart-rate --virtual-users 1 --ramp-up 0s --duration 20s --measurement-interval 1s --batch-interval 5s
go run . heart-rate --virtual-users 10 --ramp-up 5s --duration 20s --measurement-interval 1s --batch-interval 5s
```

The following flags belong to the legacy simulator command:

| Option | Default | Description |
|---|---:|---|
| `--api-base-url` | `http://localhost` | Backend/API base URL |
| `--batch-size` | `5` | Events per metric batch (1–500) |
| `--interval` | `5s` | Delay between batch cycles |
| `--duration` | `0s` | Unlimited until stopped |
| `--timeout` | `5s` | HTTP attempt timeout |
| `--max-retries` | `3` | Retries after the initial attempt (0–10) |
| `--retry-delay` | `500ms` | Delay between retries |

Every option can also be configured with its corresponding `VITALOOM_*` environment variable, for example `VITALOOM_API_BASE_URL` and `VITALOOM_BATCH_SIZE`.

## Provision test accounts

Start Nginx and the Backend, then run `go run . provision --from 1 --to 1` from the Producer repo. The account password must be at least 8 characters. If the account already exists, use its current password. Look for `count=1`; repeat the command to verify login.

To provision all 1,000 test accounts, run `go run . provision`.

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
