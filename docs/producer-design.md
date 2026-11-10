# Producer Simulation & Load Generation Design

**Status:** Proposed direction for the next development stages  
**Scope:** Synthetic Heart Rate and Steps data sent to VitaLoom Backend  
**Implementation status:** The current repository is a single-run simulator. Multi-account orchestration and multi-user load generation are not implemented yet.

This document records the intended direction for the Data Producer so implementation can proceed incrementally without redesigning the tool for each metric.

## 1. Goals

The standalone Go Producer has two related responsibilities:

1. **Data simulation:** generate synthetic health measurements for development and end-to-end checks.
2. **Load generation:** run multiple simulated users and devices concurrently to create controlled traffic through the VitaLoom Nginx ingress and against an explicitly authorized test Backend.

The Producer must send all API traffic through the configured **Nginx base URL**. It must not call the Spring Boot port directly or bypass Nginx. Nginx forwards requests to the Backend; the Producer does not need to know the Backend's internal host or port.

The Producer never connects to MongoDB or writes to Backend persistence directly. Backend remains authoritative for authentication, validation, persistence, deduplication and aggregation.

Heart Rate is the first implementation target. Steps should reuse the same scheduling, batching, HTTP, authentication and reporting capabilities after the Heart Rate path is aligned with the canonical API contract.

## 2. Concepts and responsibilities

| Concept | Meaning |
|---|---|
| **Virtual user (VU)** | A simulated user/account in a load scenario. CCU is the number of VUs active at the same time. A VU is a load-test concept, not a Backend or MongoDB entity. |
| **Device** | A simulated source that generates measurements for one VU. Each device has its own stable identity and measurement schedule. |
| **Measurement** | One generated metric observation/increment, with its own event identity and occurrence time. |
| **Batch** | A group of pending measurements sent to one metric endpoint in one HTTP request. |
| **Producer run** | One execution of the CLI, identified by a run ID for diagnostics and reporting. |
| **Worker / scheduler** | Producer execution machinery that advances VUs and devices. It is an implementation detail, not a business entity. |

### Initial VU-to-account mapping

The initial load-test target is **1,000 VUs** with:

- One goroutine representing one VU.
- One VU using one pre-provisioned Backend test account.
- One simulated device per VU.
- One distinct access token per VU/account.
- One stable `deviceId` per simulated device.

Thus, the first scenario represents 1,000 distinct test accounts and 1,000 devices. The number of goroutines is a Producer implementation choice; it does not imply one goroutine per measurement or HTTP request.

Use a deterministic test-account naming convention, for example `loadtest-0001@vitaloom.test` through `loadtest-1000@vitaloom.test`. All test accounts may share one test password supplied through environment configuration. The password must satisfy the Backend registration policy (currently at least 8 characters); never hard-code it in source or log it.

Accounts are provisioned before a benchmark run and reused. Do not register accounts as part of the timed ingestion workload. Account provisioning should be repeatable: create missing accounts, handle already-registered accounts, and verify credentials through login. It must use the same Nginx base URL as all other Producer API traffic.

A VU does not send an `accountId` in the measurement payload. Backend derives account ownership from the authenticated JWT principal. Never simulate account identity by accepting arbitrary `accountId` values from Producer payloads.

## 3. Network ingress and deployment modes

### Single ingress rule

All Producer API calls use one configured base URL:

`VITALOOM_API_BASE_URL` must point to the externally reachable **Nginx listener**, not directly to Spring Boot.

The Producer appends canonical API paths to this base URL. The Producer must not contain a separate Backend URL or hard-code Spring Boot's port (currently 8080).

The following URLs are illustrative only; use the actual host, DNS name and published Nginx port for the active environment:

| Producer runtime | Example base URL |
|---|---|
| Producer running as a process on the host while Nginx exposes a host port | `http://localhost:<nginx-host-port>` |
| Producer running inside the future Docker Compose network | `http://nginx:80` (using the actual Compose service name and listener) |

Do not assume port 8088 or any other host port until Nginx's port publishing is configured. Do not use `http://localhost:8080` as the Producer base URL when that points directly to Spring Boot.

### Nginx upstream is a separate concern

The Producer-to-Nginx URL and Nginx-to-Backend upstream URL are different network hops:

```text
Current separate-app mode

Go Producer process
    |
    | VITALOOM_API_BASE_URL
    v
Nginx listener (host/DNS + published port)
    |
    | proxy_pass upstream reachable from Nginx
    v
Spring Boot Backend process (port 8080)


Future Docker Compose mode

Producer container
    |
    | http://nginx:80
    v
Nginx service
    |
    | proxy_pass http://<backend-service>:8080
    v
Backend service
```

The configured Nginx `proxy_pass` target must be resolvable and reachable **from the Nginx runtime**. For example, `proxy_pass http://server:8080` only works when `server` resolves from Nginx's network. If Nginx runs in a container while Spring Boot runs directly on the host, the upstream must instead use a host address reachable from that container. This does not change the URL configured in Producer: Producer still calls Nginx only.

When the applications move to Docker Compose, update the network-specific Nginx upstream and Producer base URL to use Compose service DNS. Do not make Producer depend on the Backend service name.

### Nginx routing and rate limiting

- Preserve the API path when proxying. Producer calls the canonical paths such as `/api/auth/login` and `/api/heart-rate/batch` through Nginx.
- Nginx is the ingress point where routing and rate-limit policies are applied.
- Producer must not bypass Nginx, connect to the Backend port as a fallback, or send a self-asserted header to bypass rate limits.
- IP allowlisting, a dedicated Producer rate-limit zone, and exact rate values are Nginx deployment/policy decisions. They are not fixed by this Producer design yet.
- Producer must recognize HTTP 429 as rate limiting, report it distinctly, and keep retries bounded. If Nginx supplies `Retry-After`, the client should respect it within the configured retry/run limits. Do not silently treat a 429 response as a successful ingestion.
- Throughput measured by Producer is traffic through Nginx. To isolate Backend capacity from Nginx rate limits, the test environment must explicitly configure an appropriate bounded Nginx policy. To test Nginx rate limiting, run without a Producer exemption.

## 4. Producer API endpoints

The Producer uses the canonical Backend API contract through the Nginx base URL.

| Method | Path | When used |
|---|---|---|
| `GET` | `/api/health` | Optional preflight check through Nginx before a run. |
| `POST` | `/api/auth/register` | One-time test-account provisioning only; not part of timed ingestion. |
| `POST` | `/api/auth/login` | Authentication warm-up to obtain one access token per test account. |
| `POST` | `/api/auth/refresh` | Renew an access token when a run outlives the access-token lifetime. |
| `POST` | `/api/heart-rate/batch` | Initial timed workload. |
| `POST` | `/api/steps/batch` | Added in the later Steps extension. |

The Producer does not need to call the Heart Rate summary endpoint during every ingestion cycle. End-to-end read verification can be performed separately by the FE or a small verification step.

### Authentication lifecycle

1. Provision the deterministic test-account range in a separate setup operation, through Nginx.
2. Before starting the timed ingestion window, log in each account through Nginx and build an in-memory account/token pool.
3. Assign exactly one account and its token to each VU.
4. Keep login/provisioning traffic outside the timed Heart Rate ingestion measurements. Pace setup requests and do not accidentally create an authentication burst while preparing the ingestion test.
5. Refresh access tokens through Nginx when needed. Current Backend access tokens last 15 minutes; refresh tokens last 30 days.
6. Never write passwords, access tokens or refresh tokens to logs or committed configuration files.

For the initial 10-minute load scenario, the 15-minute access-token lifetime should normally cover the timed run after authentication warm-up. Refresh support is still needed for longer runs and reusable test sessions.

A separate authentication benchmark can be designed later. The first Heart Rate throughput test measures ingestion after tokens have already been prepared.

## 5. Target execution model

The target flow is:

```text
Producer run
  ├── Test account/token pool (prepared before timing starts)
  └── Virtual users (VU 1 ... VU N)
        └── One account + one device per VU (initial scenario)
              ├── Metric generator(s)
              ├── Measurement schedule
              └── Per-device pending batches
                    └── Shared authenticated HTTP client
                          └── Nginx ingress
                                └── VitaLoom Backend metric APIs
```

Each device generates measurements on its own measurement schedule. Its pending measurements are dispatched on the batch schedule. Heart Rate and Steps use separate generators and their respective Backend endpoints, while sharing common scheduling, authentication and HTTP infrastructure.

The Producer should support a simple one-VU/one-device run as well as a multi-VU scenario. Multi-VU support should extend the existing simulator rather than introduce a second standalone load-testing application.

## 6. Target CLI configuration

The following is the proposed user-facing shape, **not a command supported by the current implementation yet**:

```bash
producer run \
  --virtual-users 1000 \
  --devices-per-user 1 \
  --batch-interval 5s \
  --measurement-interval 1s \
  --ramp-up 30s \
  --duration 10m
```

The Nginx ingress URL is supplied separately, for example through `VITALOOM_API_BASE_URL=http://localhost:<nginx-host-port>`. Do not put the Backend's direct port in this setting.

| Option | Intended meaning |
|---|---|
| `--virtual-users` | Number of test accounts/VUs in the scenario; initial target is 1,000. |
| `--devices-per-user` | Number of independent simulated devices per VU; initially 1. |
| `--measurement-interval` | Time between generated measurements for each device. |
| `--batch-interval` | How often each device flushes pending measurements through Nginx to the Backend. |
| `--ramp-up` | Time over which VUs are introduced gradually instead of starting all at once. |
| `--duration` | Length of the bounded scenario run; initial proposal is 10 minutes including ramp-up. |

The existing `--timeout`, `--max-retries` and `--retry-delay` settings remain relevant. The existing fixed `--batch-size` behavior may need to become a maximum batch-size safeguard when generation and flushing use independent schedules. Heart Rate requests must stay within the API contract limit of 500 events per batch.

### Timing semantics

- **Measurement interval** controls data-generation frequency.
- **Batch interval** controls network submission frequency.
- They are independent. For example, a 1-second measurement interval and 5-second batch interval normally produces about five measurements per device per batch.
- A device should retain pending events until they are submitted or the run is stopped according to the agreed shutdown policy.
- Retry must reuse the same logical events, IDs and timestamps. A retry must not generate replacement measurements.
- Initial flush times should be distributed across the batch interval to avoid an accidental synchronized burst from all 1,000 VUs. A separate scenario may deliberately synchronize them when testing burst behavior.

## 7. Identity, timestamps and retry behavior

- A device ID identifies the simulated device and remains stable across Producer runs when the same device is reused.
- A run ID identifies one Producer execution and is diagnostic metadata; it is not device identity or measurement identity.
- Every measurement has a stable event ID. It is created once and reused for all retries of that measurement.
- `occurredAt` represents the simulated measurement time and is sent in UTC. It is independent of the time at which the Producer sends the batch.
- Backend creates `receivedAt`; the Producer must not send or control it.
- The Producer must not infer acceptance from an HTTP success alone. It should parse the metric-specific response and account for accepted, duplicate and rejected outcomes.
- Rate-limit responses (HTTP 429) and exhausted retries must remain visible in the run report.

The current implementation preserves the serialized request body during retries within the running process. Persisting pending batches across a Producer process restart is not part of the current implementation and should not be assumed.

## 8. Load-test measurements

The load-generation mode should report enough information to understand both generated load and ingestion outcomes.

| Measurement | Meaning |
|---|---|
| Active VUs / peak CCU | Current and maximum number of active simulated users. |
| Active devices | Number of device simulations currently running. |
| Request rate | HTTP requests per second, overall and per endpoint, after Producer submission. |
| Measurement rate | Measurements generated, submitted and accepted per second, per metric. |
| Ingestion outcomes | Accepted, duplicate and rejected event counts/rates. |
| Rate limited | HTTP 429 count/rate, separated from other HTTP failures. |
| Request latency | At least p50, p95 and p99 per endpoint; identify that this is Producer-observed latency through Nginx. |
| Failures and retries | HTTP failures, exhausted retries and retry attempts. |
| Run summary | Configuration, elapsed time, totals and final status. |

CCU, RPS and measurement throughput are different measurements. The number of VUs alone does not determine request rate; device count, metric count, measurement interval, batch interval and Nginx policy all affect observed traffic.

For a simplified scenario with 1,000 active devices, a 5-second batch interval and one request per device per interval:

- Heart Rate alone produces approximately 200 batch requests/second.
- If both Heart Rate and Steps each send one batch per device per interval, the combined rate is approximately 400 requests/second (about 200 per endpoint).
- With a 1-second measurement interval, Heart Rate generates approximately 1,000 measurements/second; both metrics together generate approximately 2,000 metric events/second.

These are steady-state planning estimates, not guaranteed throughput. Ramp-up, request duration, retries, scheduling delays, Nginx rate limits, batch limits and Backend response times change the observed values.

Producer-side latency and outcome metrics do not replace Nginx or Backend monitoring. CPU, memory, database load, Nginx accepted/rejected requests and server-side saturation must be observed separately when evaluating Backend capacity.

## 9. Current implementation baseline

The current code is intentionally smaller than the target design.

| Area | Current state |
|---|---|
| Runtime model | One Producer process, one run ID, one generator and one API client. |
| Metrics | Generates Heart Rate and Steps data in the same cycle. |
| Generation | Synthetic values use simple hard-coded activity phases. |
| Timing | One `--interval` controls batch cycles. Event timestamps are distributed within a batch using `interval / batchSize`; there is no independent measurement scheduler. |
| Sending | Heart Rate and Steps batches are sent sequentially. |
| Authentication | No login/token pool or Bearer token support in the current Producer client. |
| Network | Current API base URL is used directly by the client; Nginx ingress is not yet an explicit documented/runtime requirement in the implementation. |
| Retry | Bounded HTTP retries; the same serialized body is reused within the current process. HTTP 429 is retryable with the current fixed delay; `Retry-After` is not parsed. |
| Reporting | Per-batch logs and a run summary with generated, accepted, duplicate, retry and failure counts. |
| Tests / CI | Unit tests cover configuration, generation, client behavior and producer lifecycle. GitHub Actions runs test, vet and build. |

Current CLI options are `--api-base-url`, `--batch-size`, `--interval`, `--duration`, `--timeout`, `--max-retries` and `--retry-delay`. The current entry point does not have a `producer run` subcommand.

### Contract alignment required before the next Heart Rate integration run

The current Producer code predates the latest Heart Rate batch contract:

- Its Heart Rate batch payload does not yet include the required `deviceId`.
- Its response decoder only understands `accepted` and `duplicates`; the current Heart Rate contract also returns `rejected` and per-item `results`.
- The current client does not send Bearer credentials. Heart Rate currently requires authentication.
- The current Producer must be configured to call the Nginx listener, not the Backend's direct Spring Boot port.
- The current Steps payload/response follows the existing Steps contract; do not change it as part of Heart Rate alignment unless that contract is separately updated.

The canonical request/response definitions are maintained in the [Backend API Contract](https://github.com/vitaloom/vitaloom-backend/blob/main/docs/api/api-contract.md). This document must not become a competing API specification.

## 10. Incremental implementation plan

1. **Confirm the Nginx ingress endpoint for the current separate-app setup.** Configure Nginx's listener and its upstream to the reachable Backend address. Confirm a health request and the authentication/Heart Rate routes through Nginx. Producer only receives the Nginx base URL.
2. **Add test-account provisioning and authentication support.** Prepare the deterministic 1,000-account range outside benchmark timing. Add login warm-up and an in-memory token pool, with refresh support for longer runs. All calls go through Nginx.
3. **Align the Heart Rate integration.** Add stable device identity to the Heart Rate batch, support its partial-acceptance response, send Bearer tokens and verify the one-account flow end to end through Nginx.
4. **Separate generation from batching.** Introduce independent measurement and batch-flush schedules for one device. Verify timing, bounded buffers, shutdown and retry behavior.
5. **Add multi-VU and multi-device orchestration.** Introduce gradual ramp-up, bounded duration, safe concurrency limits, one account/token/device per initial VU and staggered initial batch timing. Initial target: 1,000 VUs.
6. **Add load-test reporting.** Report CCU, requests/second, measurements/second, latency distributions, accepted/duplicate/rejected outcomes, HTTP 429 responses and failures.
7. **Extend the shared pipeline to Steps.** Keep metric-specific payloads and response handling, while reusing account/VU scheduling, batching, transport, authentication, retry and reporting infrastructure.

Do not implement profile-driven behavioral scenarios, UI-controlled simulation, distributed workers, Kafka or a separate processing service as prerequisites for the first load test.

### Deployment evolution

- **Now:** Backend and Producer run as separate applications. Producer calls the Nginx listener; Nginx proxies to the Backend address reachable from the Nginx runtime.
- **Later:** Move Producer, Nginx and Backend into Docker Compose. Change only environment/network configuration so Producer calls the Nginx Compose service address (for example `http://nginx:80`); do not change the Producer's API paths or introduce a direct Backend connection.

## 11. Safety and operating boundaries

- Load generation must require an explicitly configured and authorized test target. It must not silently target production.
- Provide bounded defaults, a maximum concurrency guard and a clear stop mechanism before enabling multi-user load.
- Keep retries bounded. A retry reuses the same event IDs and payload.
- Start with small local scenarios and increase load deliberately (for example 100, 250, 500 and then 1,000 VUs).
- Do not connect the Producer directly to MongoDB or bypass Backend validation or Nginx ingress.
- Treat Nginx HTTP 429 as a rate-limit outcome, not an accepted measurement.
- Synthetic data is for development and testing only; it is not real wearable data, a clinical measurement or medical advice.

## 12. Decisions to confirm when implementing load generation

The implementation can follow the direction above. The following operational details should be finalized against the actual test environment before enabling larger runs:

1. The exact Nginx host/DNS name and published listener port used by the current separate-app setup.
2. The reachable Backend upstream address configured from Nginx's runtime. This is not the Producer's base URL.
3. The Nginx rate-limit policy for a throughput test versus a rate-limit test. No IP bypass/header mechanism is assumed by this design.
4. The test password value supplied securely through environment configuration.
5. The final maximum VU/concurrency defaults based on the local Producer machine and target Backend capacity.

## References

- [Current implementation guide](../README.md)
- [Repository agent instructions](../AGENTS.md)
- [Canonical Backend API Contract](https://github.com/vitaloom/vitaloom-backend/blob/main/docs/api/api-contract.md)
