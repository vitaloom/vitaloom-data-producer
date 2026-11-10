package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestHeartRateOnceLogsPartialResultsAndReadsSummaryWithSameToken(t *testing.T) {
	var loginCalls, batchCalls, summaryCalls atomic.Int32
	var receivedBatch authenticatedHeartRateBatch
	var workflowLogs strings.Builder
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/auth/login":
			loginCalls.Add(1)
			var request authRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("decode login request: %v", err)
			}
			if request.Email != "loadtest-0001@vitaloom.test" || request.Password != "test-password-123" {
				t.Errorf("unexpected login identity: email=%q", request.Email)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"accessToken":"access-token","refreshToken":"refresh-token","expiresInSeconds":900}`)
		case "/api/heart-rate/batch":
			batchCalls.Add(1)
			assertBearer(t, r, "access-token")
			rawBody, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("read batch request: %v", err)
			}
			if err := json.Unmarshal(rawBody, &receivedBatch); err != nil {
				t.Errorf("decode batch request: %v", err)
			}
			if r.Method != http.MethodPost || r.Header.Get("Content-Type") != "application/json" {
				t.Errorf("unexpected batch request: method=%s content-type=%q", r.Method, r.Header.Get("Content-Type"))
			}
			var raw map[string]json.RawMessage
			_ = json.Unmarshal(rawBody, &raw)
			if _, ok := raw["accountId"]; ok {
				t.Error("batch payload must not contain accountId")
			}
			if receivedBatch.DeviceID != "device-loadtest-0001" || len(receivedBatch.Events) != heartRateSampleCount {
				t.Errorf("unexpected batch identity or event count: %+v", receivedBatch)
			}
			receipt := time.Now().UTC()
			for _, event := range receivedBatch.Events {
				if event.OccurredAt.Location() != time.UTC || event.OccurredAt.Nanosecond()%int(time.Millisecond) != 0 {
					t.Errorf("occurredAt is not UTC millisecond precision: %s", event.OccurredAt)
				}
				if event.OccurredAt.After(receipt) {
					t.Errorf("occurredAt %s is later than batch receipt %s", event.OccurredAt, receipt)
				}
			}
			if receivedBatch.Events[0].EventID == receivedBatch.Events[1].EventID || receivedBatch.Events[1].EventID == receivedBatch.Events[2].EventID {
				t.Error("event IDs must be unique")
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(authenticatedHeartRateResponse{
				RunID: receivedBatch.RunID, Received: 3, Accepted: 1, Duplicates: 1, Rejected: 1,
				Results: []heartRateItemResult{
					{Index: 0, EventID: receivedBatch.Events[0].EventID, Status: "ACCEPTED"},
					{Index: 1, EventID: receivedBatch.Events[1].EventID, Status: "DUPLICATE"},
					{Index: 2, EventID: receivedBatch.Events[2].EventID, Status: "REJECTED", Error: &heartRateItemError{Code: "EVENT_ID_CONFLICT", Message: "conflicting event identity"}},
				},
			})
		case "/api/heart-rate/summary":
			summaryCalls.Add(1)
			assertBearer(t, r, "access-token")
			if r.Method != http.MethodGet {
				t.Errorf("summary method = %s; want GET", r.Method)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"date":"2026-11-10","timezone":"Asia/Ho_Chi_Minh","asOf":"2026-11-10T12:00:00Z","latestRecorded":{"heartRateBpm":88,"occurredAt":"2026-11-10T11:59:00Z"},"dailyAverage":{"heartRateBpm":81.5,"dataStatus":"CURRENT_DAY_PARTIAL"}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	cfg := Config{APIBaseURL: server.URL, Timeout: time.Second, MaxRetries: 2, RetryDelay: 0}
	err := runHeartRateOnce(context.Background(), cfg, "test-password-123", 1, log.New(&workflowLogs, "", 0))
	if err == nil || !strings.Contains(err.Error(), "1 rejected item") {
		t.Fatalf("runHeartRateOnce() error = %v; want reported partial rejection", err)
	}
	if loginCalls.Load() != 1 || batchCalls.Load() != 1 || summaryCalls.Load() != 1 {
		t.Fatalf("calls login=%d batch=%d summary=%d; want one each", loginCalls.Load(), batchCalls.Load(), summaryCalls.Load())
	}
	logs := workflowLogs.String()
	for _, expected := range []string{"status=ACCEPTED", "status=DUPLICATE", "status=REJECTED error_code=EVENT_ID_CONFLICT", "latest_bpm=88", "daily_average=81.50"} {
		if !strings.Contains(logs, expected) {
			t.Errorf("workflow logs missing %q: %s", expected, logs)
		}
	}
}

func TestAuthenticatedHeartRateRetryAndPermanentStatusHandling(t *testing.T) {
	tests := []struct {
		name           string
		status         int
		retryAfter     string
		wantCalls      int32
		wantRateLimits int
		wantErrStatus  string
	}{
		{name: "401 stops immediately", status: http.StatusUnauthorized, wantCalls: 1, wantErrStatus: "HTTP 401"},
		{name: "429 honors zero Retry-After and retries", status: http.StatusTooManyRequests, retryAfter: "0", wantCalls: 2, wantRateLimits: 1},
		{name: "5xx retries with same body", status: http.StatusServiceUnavailable, wantCalls: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var calls atomic.Int32
			var firstBody string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				call := calls.Add(1)
				assertBearer(t, r, "test-token")
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Errorf("read request body: %v", err)
				}
				if call == 1 {
					firstBody = string(body)
				} else if string(body) != firstBody {
					t.Errorf("retry body changed: first=%s retry=%s", firstBody, body)
				}
				if call == 1 {
					if tt.retryAfter != "" {
						w.Header().Set("Retry-After", tt.retryAfter)
					}
					w.WriteHeader(tt.status)
					_, _ = io.WriteString(w, `{"error":{"message":"this body must not be logged"}}`)
					return
				}
				var batch authenticatedHeartRateBatch
				if err := json.Unmarshal(body, &batch); err != nil {
					t.Errorf("decode retry payload: %v", err)
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(authenticatedHeartRateResponse{
					RunID: batch.RunID, Received: 1, Accepted: 1,
					Results: []heartRateItemResult{{Index: 0, EventID: batch.Events[0].EventID, Status: "ACCEPTED"}},
				})
			}))
			defer server.Close()
			var logs strings.Builder
			client := newAuthenticatedHeartRateClient(Config{APIBaseURL: server.URL, Timeout: time.Second, MaxRetries: 1, RetryDelay: 0}, log.New(&logs, "", 0))
			batch := authenticatedHeartRateBatch{DeviceID: "device-1", RunID: "run-retry", Events: []HeartRateEvent{{EventID: "event-1", OccurredAt: time.Now().UTC().Truncate(time.Millisecond), HeartRateBPM: 75}}}
			_, status, err := client.sendBatch(context.Background(), "test-token", batch)
			if calls.Load() != tt.wantCalls {
				t.Fatalf("requests=%d; want %d", calls.Load(), tt.wantCalls)
			}
			if client.rateLimits != tt.wantRateLimits {
				t.Errorf("rate limits=%d; want %d", client.rateLimits, tt.wantRateLimits)
			}
			if tt.wantErrStatus != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErrStatus) || status != tt.status {
					t.Fatalf("sendBatch() = status %d, error %v; want status %d", status, err, tt.status)
				}
			} else if err != nil || status != http.StatusOK {
				t.Fatalf("sendBatch() = status %d, error %v; want successful retry", status, err)
			}
			if strings.Contains(logs.String(), "this body must not be logged") {
				t.Errorf("raw error body leaked to logs: %s", logs.String())
			}
		})
	}
}

func assertBearer(t *testing.T, r *http.Request, token string) {
	t.Helper()
	if got := r.Header.Get("Authorization"); got != "Bearer "+token {
		t.Errorf("Authorization = %q; want Bearer token", got)
	}
}
