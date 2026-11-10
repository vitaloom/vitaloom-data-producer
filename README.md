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

Without a selector, the Producer runs both metrics. Use `step` or `heart-rate` to run one metric. Both sample once per second; `--batch-interval` controls when each stream flushes. Every run uses authenticated scheduled ingestion. The multi-user example requires ten provisioned accounts.

```powershell
go run . --account 1 --duration 30s
go run . step --account 1 --duration 30s
go run . heart-rate --account 1 --batch-interval 5s --duration 30s
go run . --virtual-users 10 --ramp-up 5s --duration 20s --batch-interval 5s
```

Options and defaults. Flags take precedence over environment values, which take precedence over defaults. `--batch-interval` takes precedence over the compatibility alias `--interval`; at the environment level `VITALOOM_BATCH_INTERVAL` takes precedence over `VITALOOM_INTERVAL`.

| Option | Default | Description |
|---|---|---:|---|
| `--api-base-url` | required | Authorized test Nginx URL; can also use `VITALOOM_API_BASE_URL` |
| `--account` | `1` | Provisioned account index (1–1000); mutually exclusive with `--virtual-users` |
| `--virtual-users` | unset | Run accounts 1 through N (1–1000); can also use `VITALOOM_VIRTUAL_USERS` |
| `--devices-per-user` | `1` | Only one simulated device per account is supported |
| `--ramp-up` | `0s` | Stagger VU starts; can also use `VITALOOM_RAMP_UP` |
| `--batch-size` | `5` | Maximum events in each metric request (1–500) |
| `--batch-interval` | `5s` | Shared flush cadence for every selected metric; can use `VITALOOM_BATCH_INTERVAL` |
| `--interval` | alias | Compatibility alias for `--batch-interval`; environment fallback `VITALOOM_INTERVAL` |
| `--duration` | `0s` | Run until stopped when zero; can also use `VITALOOM_DURATION` |
| `--timeout` | `5s` | Per-request timeout; can also use `VITALOOM_TIMEOUT` |
| `--max-retries` | `3` | Retries after the initial request (0–10); can also use `VITALOOM_MAX_RETRIES` |
| `--retry-delay` | `500ms` | Delay between retry attempts; can also use `VITALOOM_RETRY_DELAY` |

All runs use one-second observations. There is no configurable measurement interval. `VITALOOM_TEST_PASSWORD` is required for every run. `provision` remains a separate account setup command.

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
