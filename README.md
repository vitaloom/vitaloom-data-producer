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

Run from the repository root. Set `VITALOOM_API_BASE_URL` to your authorized Nginx listener and replace the example value with the actual host and port. For local Nginx on its default HTTP port, use `http://localhost`; do not point the Producer directly at Spring Boot. This initial ingestion command preserves its legacy behavior and does not yet log in or send Bearer tokens (authenticated Heart Rate integration is Phase 2).

```powershell
$env:VITALOOM_API_BASE_URL = 'http://localhost:<nginx-port>'
go run . --batch-size 5 --interval 2s --duration 90s
```

Omit `--duration` or set it to `0s` to run until Ctrl+C.

The current defaults are:

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

Start Nginx and the Backend, then open PowerShell in the Producer repo.

1. **Set the test Nginx URL.** Change it below if your listener uses a different port or hostname.

   ```powershell
   $env:VITALOOM_API_BASE_URL = 'http://localhost'
   ```

2. **Try one account.** Enter a test password of at least 8 characters when prompted. If `loadtest-0001@vitaloom.test` already exists, enter its current password.

   ```powershell
   $securePassword = Read-Host 'Test account password' -AsSecureString
   $env:VITALOOM_TEST_PASSWORD = (New-Object System.Net.NetworkCredential('', $securePassword)).Password
   try {
       go run . provision --from 1 --to 1
   } finally {
       Remove-Item Env:VITALOOM_TEST_PASSWORD -ErrorAction SilentlyContinue
       Remove-Variable securePassword -ErrorAction SilentlyContinue
   }
   ```

3. **Look for `count=1`.** Repeat step 2 with the same password to verify that the account can log in.

To provision all 1,000 test accounts, replace the command inside `try` with `go run . provision`.

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
