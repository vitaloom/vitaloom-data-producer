# Producer Simulation & Load Generation Design

**Status:** Proposed direction for the next development stages  
**Scope:** Synthetic Heart Rate and Steps data sent to VitaLoom Backend  
**Implementation status:** The current repository is a single-run simulator. The multi-user load-generation design below is not implemented yet.

This document records the intended direction for the Data Producer so that implementation can proceed incrementally without redesigning the tool for each metric.

## 1. Goals

The standalone Go Producer has two related responsibilities:

1. **Data simulation:** generate plausible synthetic health measurements for development and end-to-end checks.
2. **Load generation:** run multiple simulated users and devices concurrently to create controlled traffic against an explicitly authorized test Backend.

The Producer sends data through Backend HTTP APIs. It never connects to MongoDB or writes to Backend persistence directly. Backend remains authoritative for validation, deduplication, persistence and aggregation.

Heart Rate is the first implementation target. Steps should reuse the same scheduling, batching, HTTP, retry and reporting capabilities after the Heart Rate path is aligned with the canonical API contract.

## 2. Concepts and responsibilities

| Concept | Meaning |
|---|---|
| **Virtual user (VU)** | A simulated active user/session in a load scenario. CCU is the number of VUs active at the same time. A VU is a load-test concept, not a MongoDB entity. |
| **Device** | A simulated source that generates measurements for a VU. Each device has its own stable identity and measurement schedule. |
| **Measurement** | One generated metric observation/increment, with its own event identity and occurrence time. |
| **Batch** | A group of pending measurements sent to one metric endpoint in one HTTP request. |
| **Producer run** | One execution of the CLI, identified by a run ID for diagnostics and reporting. |
| **Worker / scheduler** | Producer execution machinery that advances VUs and devices. It is an implementation detail, not a business entity. |

A VU may own one or more simulated devices. The initial load-test example uses one device per VU.

A VU does not automatically mean a distinct real Backend account. A scenario may use a shared test account or multiple test accounts. Multi-account isolation testing requires valid credentials and Backend account isolation; it must not be simulated by accepting arbitrary `accountId` values in payloads.

For Heart Rate, each batch contains measurements from exactly one device and includes its stable `deviceId`. The current Steps API contract does not define a `deviceId` field. Do not add one to Steps requests independently; revisit that contract only if device-level Steps identity is required.

## 3. Target execution model

The target flow is:

```text
Producer run
  └── Virtual users (VU 1 ... VU N)
        └── Devices per VU
              ├── Metric generator(s)
              ├── Measurement schedule
              └── Per-device pending batches
                    └── Shared ingestion client
                          └── VitaLoom Backend metric APIs
```

Each device generates measurements on its own measurement schedule. Its pending measurements are dispatched on the batch schedule. Heart Rate and Steps use separate generators and their respective Backend endpoints, while sharing common scheduling and HTTP infrastructure.

The Producer should support a simple one-VU/one-device run as well as a multi-VU scenario. Multi-VU support should extend the existing simulator rather than introduce a second standalone load-testing application.

## 4. Target CLI configuration

The following is the proposed user-facing shape, **not a command supported by the current implementation yet**:

```bash
producer run \
  --virtual-users 100 \
  --devices-per-user 1 \
  --batch-interval 5s \
  --measurement-interval 1s \
  --ramp-up 30s \
  --duration 10m
```

| Option | Intended meaning |
|---|---|
| `--virtual-users` | Number of simulated users to start for the scenario. |
| `--devices-per-user` | Number of independent simulated devices per VU. |
| `--measurement-interval` | Time between generated measurements for each device. |
| `--batch-interval` | How often each device flushes pending measurements to the Backend. |
| `--ramp-up` | Time over which VUs are introduced gradually instead of starting all at once. |
| `--duration` | Length of the bounded scenario run. Exact interaction with ramp-up should be finalized before implementing the CLI. |

The existing `--api-base-url`, `--timeout`, `--max-retries` and `--retry-delay` settings remain relevant. The existing fixed `--batch-size` behavior may need to become a maximum batch-size safeguard when generation and flushing use independent schedules. Heart Rate requests must stay within the API contract limit of 500 events per batch.

### Timing semantics

- **Measurement interval** controls data-generation frequency.
- **Batch interval** controls network submission frequency.
- They are independent. For example, a 1-second measurement interval and 5-second batch interval normally produces about five measurements per device per batch.
- A device should retain pending events until they are submitted or the run is stopped according to the agreed shutdown policy.
- Retry must reuse the same logical events, IDs and timestamps. A retry must not generate replacement measurements.

## 5. Identity, timestamps and retry behavior

- A device ID identifies the simulated device and remains stable across Producer runs when the same device is reused.
- A run ID identifies one Producer execution and is diagnostic metadata; it is not device identity or measurement identity.
- Every measurement has a stable event ID. It is created once and reused for all retries of that measurement.
- `occurredAt` represents the simulated measurement time and is sent in UTC. It is independent of the time at which the Producer sends the batch.
- Backend creates `receivedAt`; the Producer must not send or control it.
- The Producer must not infer acceptance from an HTTP success alone. It should parse the metric-specific response and account for accepted, duplicate and rejected outcomes.

The current implementation preserves the serialized request body during retries within the running process. Persisting pending batches across a Producer process restart is not part of the current implementation and should not be assumed.

## 6. Load-test measurements

The load-generation mode should report enough information to understand both generated load and ingestion outcomes.

| Measurement | Meaning |
|---|---|
| Active VUs / peak CCU | Current and maximum number of active simulated users. |
| Active devices | Number of device simulations currently running. |
| Request rate | HTTP requests per second, overall and per endpoint. |
| Measurement rate | Measurements generated and submitted per second, per metric. |
| Ingestion outcomes | Accepted, duplicate and rejected event counts/rates. |
| Request latency | At least p50, p95 and p99 per endpoint. |
| Failures and retries | HTTP failures, exhausted retries and retry attempts. |
| Run summary | Configuration, elapsed time, totals and final status. |

CCU, RPS and measurement throughput are different measurements. The number of VUs alone does not determine request rate; device count, metric count, measurement interval and batch interval all affect traffic.

For a simplified scenario with 100 active devices, a 5-second batch interval and one request per device per interval:

- Heart Rate alone produces approximately 20 batch requests/second.
- If both Heart Rate and Steps each send one batch per device per interval, the combined rate is approximately 40 requests/second (about 20 per endpoint).
- With a 1-second measurement interval, each metric generates approximately 100 measurements/second; both metrics together generate approximately 200 metric events/second.

These are planning estimates, not guaranteed throughput. Ramp-up, request duration, retries, scheduling delays, batch limits and Backend response times change the observed values.

Producer-side latency and outcome metrics do not replace Backend-side monitoring. CPU, memory, database load and server-side saturation must be observed separately when evaluating Backend capacity.

## 7. Current implementation baseline

The current code is intentionally smaller than the target design.

| Area | Current state |
|---|---|
| Runtime model | One Producer process, one run ID, one generator and one API client. |
| Metrics | Generates Heart Rate and Steps data in the same cycle. |
| Generation | Synthetic values use simple hard-coded activity phases. |
| Timing | One `--interval` controls batch cycles. Event timestamps are distributed within a batch using `interval / batchSize`; there is no independent measurement scheduler. |
| Sending | Heart Rate and Steps batches are sent sequentially. |
| Retry | Bounded HTTP retries; the same serialized body is reused within the current process. |
| Reporting | Per-batch logs and a run summary with generated, accepted, duplicate, retry and failure counts. |
| Tests / CI | Unit tests cover configuration, generation, client behavior and producer lifecycle. GitHub Actions runs test, vet and build. |

Current CLI options are `--api-base-url`, `--batch-size`, `--interval`, `--duration`, `--timeout`, `--max-retries` and `--retry-delay`. The current entry point does not have a `producer run` subcommand.

### Contract alignment required before the next Heart Rate integration run

The current Producer code predates the latest Heart Rate batch contract:

- Its Heart Rate batch payload does not yet include the required `deviceId`.
- Its response decoder only understands `accepted` and `duplicates`; the current Heart Rate contract also returns `rejected` and per-item `results`.
- The current client does not send Bearer credentials. Do not use it for multi-account personal-data ingestion until the Backend ingestion path enforces the authenticated account context and the Producer can supply valid test credentials.
- The current Steps payload/response follows the existing Steps contract; do not change it as part of Heart Rate alignment unless that contract is separately updated.

The canonical request/response definitions are maintained in the [Backend API Contract](https://github.com/vitaloom/vitaloom-backend/blob/main/docs/api/api-contract.md). This document must not become a competing API specification.

## 8. Incremental implementation plan

1. **Align the Heart Rate integration.** Add the stable device identity to the Heart Rate batch, support its partial-acceptance response, and resolve the authentication/test-account prerequisite for the intended test environment.
2. **Separate generation from batching.** Introduce an independent measurement schedule and batch flush schedule for one device. Verify the one-device Heart Rate flow end to end.
3. **Extend the shared pipeline to Steps.** Keep metric-specific payloads and response handling, while reusing scheduling, batching, transport, retry and reporting infrastructure.
4. **Add multi-VU and multi-device orchestration.** Introduce gradual ramp-up, bounded scenario duration, safe concurrency limits and independent device state.
5. **Add load-test reporting and profiles.** Report CCU, request/measurement rates, latency distributions, ingestion outcomes and failures. Add reusable workload profiles only when real test scenarios require them.

Do not implement profile-driven behavioral scenarios, UI-controlled simulation, distributed workers, Kafka or a separate processing service as prerequisites for the first load test.

## 9. Safety and operating boundaries

- Load generation must require an explicitly configured and authorized test target. It must not silently target production.
- Provide bounded defaults, a maximum concurrency guard and a clear stop mechanism before enabling multi-user load.
- Keep retries bounded. A retry reuses the same event IDs and payload.
- Start with small local scenarios and increase load deliberately.
- Do not connect the Producer directly to MongoDB or bypass Backend validation.
- Synthetic data is for development and testing only; it is not real wearable data, a clinical measurement or medical advice.

## 10. Decisions to confirm when implementing load generation

The design direction is recorded, but these details should be settled against the actual test scenarios before code is added:

1. Whether `--duration` includes the ramp-up period or measures the steady-state period after ramp-up.
2. Whether a load scenario uses one shared test account or a pool of test accounts/tokens. Multi-account testing depends on enforced Backend authentication and account isolation.
3. Whether the first load-test milestone targets Heart Rate only or sends Heart Rate and Steps together.
4. The default and maximum VU/device/concurrency limits, based on the local test machine and target Backend capacity.

## References

- [Current implementation guide](../README.md)
- [Repository agent instructions](../AGENTS.md)
- [Canonical Backend API Contract](https://github.com/vitaloom/vitaloom-backend/blob/main/docs/api/api-contract.md)
