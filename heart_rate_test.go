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

func TestAuthenticatedHeartRateRetryPreservesBodyAndHandlesStatuses(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		retryAfter string
		wantCalls  int32
		wantErr    string
	}{
		{name: "unauthorized is permanent", status: http.StatusUnauthorized, wantCalls: 1, wantErr: "HTTP 401"},
		{name: "429 retries", status: http.StatusTooManyRequests, retryAfter: "0", wantCalls: 2},
		{name: "server error retries", status: http.StatusServiceUnavailable, wantCalls: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var calls atomic.Int32
			var firstBody string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assertBearer(t, r, "token")
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Errorf("read request: %v", err)
				}
				call := calls.Add(1)
				if call == 1 {
					firstBody = string(body)
					if tt.retryAfter != "" {
						w.Header().Set("Retry-After", tt.retryAfter)
					}
					w.WriteHeader(tt.status)
					return
				}
				if string(body) != firstBody {
					t.Errorf("retry body changed: first=%s next=%s", firstBody, body)
				}
				var batch authenticatedHeartRateBatch
				if err := json.Unmarshal(body, &batch); err != nil {
					t.Errorf("decode batch: %v", err)
				}
				writeHeartRateResponse(w, batch, []string{"ACCEPTED"}, 1, 0, 0)
			}))
			defer server.Close()
			client := newAuthenticatedHeartRateClient(Config{APIBaseURL: server.URL, Timeout: time.Second, MaxRetries: 1, RetryDelay: 0}, log.New(io.Discard, "", 0))
			batch := authenticatedHeartRateBatch{DeviceID: "device-1", RunID: "run-1", Events: []HeartRateEvent{{EventID: "event-1", OccurredAt: time.Now().UTC().Truncate(time.Millisecond), HeartRateBPM: 75}}}
			_, status, err := client.sendBatch(context.Background(), "token", batch)
			if calls.Load() != tt.wantCalls {
				t.Fatalf("calls=%d; want %d", calls.Load(), tt.wantCalls)
			}
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) || status != tt.status {
					t.Fatalf("sendBatch() = status %d err=%v; want %q", status, err, tt.wantErr)
				}
			} else if err != nil || status != http.StatusOK {
				t.Fatalf("sendBatch() = status %d err=%v; want success", status, err)
			}
		})
	}
}

func TestValidateHeartRateResponseChecksIdentityStatusAndCounters(t *testing.T) {
	batch := authenticatedHeartRateBatch{RunID: "run", Events: []HeartRateEvent{{EventID: "event-a"}, {EventID: "event-b"}}}
	good := authenticatedHeartRateResponse{RunID: "run", Received: 2, Accepted: 1, Rejected: 1, Results: []heartRateItemResult{
		{Index: 0, EventID: "event-a", Status: "ACCEPTED"},
		{Index: 1, EventID: "event-b", Status: "REJECTED", Error: &heartRateItemError{Code: "INVALID"}},
	}}
	if err := validateHeartRateResponse(batch, good); err != nil {
		t.Fatalf("valid partial response rejected: %v", err)
	}
	bad := good
	bad.Results = append([]heartRateItemResult(nil), good.Results...)
	bad.Results[1].EventID = "wrong"
	if err := validateHeartRateResponse(batch, bad); err == nil {
		t.Fatal("inconsistent item identity was accepted")
	}
	bad = good
	bad.Accepted = 2
	if err := validateHeartRateResponse(batch, bad); err == nil {
		t.Fatal("inconsistent result counters were accepted")
	}
}

func writeHeartRateResponse(w http.ResponseWriter, batch authenticatedHeartRateBatch, statuses []string, accepted, duplicates, rejected int) {
	results := make([]heartRateItemResult, len(batch.Events))
	for i, event := range batch.Events {
		results[i] = heartRateItemResult{Index: i, EventID: event.EventID, Status: statuses[i]}
		if statuses[i] == "REJECTED" {
			results[i].Error = &heartRateItemError{Code: "INVALID_EVENT", Message: "invalid test event"}
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(authenticatedHeartRateResponse{RunID: batch.RunID, Received: len(batch.Events), Accepted: accepted, Duplicates: duplicates, Rejected: rejected, Results: results})
}
