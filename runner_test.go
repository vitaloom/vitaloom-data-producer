package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type runCapture struct {
	mu           sync.Mutex
	loginCounts  map[string]int
	hrBatches    []authenticatedHeartRateBatch
	stepsBatches []authenticatedStepsBatch
	accounts     map[string]string
	summaryHits  int
}

type runLogWriter func([]byte) (int, error)

func (w runLogWriter) Write(p []byte) (int, error) { return w(p) }

func newRunServer(t *testing.T, capture *runCapture, delay time.Duration) *httptest.Server {
	t.Helper()
	if capture.loginCounts == nil {
		capture.loginCounts = make(map[string]int)
	}
	if capture.accounts == nil {
		capture.accounts = make(map[string]string)
	}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/auth/login":
			var request authRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("decode login: %v", err)
			}
			capture.mu.Lock()
			capture.loginCounts[request.Email]++
			capture.accounts[request.Email] = "access-" + request.Email
			capture.mu.Unlock()
			_ = json.NewEncoder(w).Encode(authResponse{AccessToken: "access-" + request.Email, RefreshToken: "refresh-" + request.Email, ExpiresInSeconds: 900})
		case "/api/auth/refresh":
			var request refreshRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("decode refresh: %v", err)
			}
			_ = json.NewEncoder(w).Encode(refreshResponse{AccessToken: "refreshed-" + request.RefreshToken, ExpiresInSeconds: 900})
		case "/api/heart-rate/batch":
			token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			var batch authenticatedHeartRateBatch
			if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
				t.Errorf("decode Heart Rate batch: %v", err)
			}
			if !strings.HasPrefix(token, "access-loadtest-") && !strings.HasPrefix(token, "refreshed-refresh-") {
				t.Errorf("unexpected Heart Rate token %q", token)
			}
			if delay > 0 {
				time.Sleep(delay)
			}
			capture.mu.Lock()
			capture.hrBatches = append(capture.hrBatches, batch)
			capture.mu.Unlock()
			writeHeartRateResponse(w, batch, repeatStatus("ACCEPTED", len(batch.Events)), len(batch.Events), 0, 0)
		case "/api/steps/batch":
			token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			var batch authenticatedStepsBatch
			if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
				t.Errorf("decode Steps batch: %v", err)
			}
			if !strings.HasPrefix(token, "access-loadtest-") && !strings.HasPrefix(token, "refreshed-refresh-") {
				t.Errorf("unexpected Steps token %q", token)
			}
			if delay > 0 {
				time.Sleep(delay)
			}
			capture.mu.Lock()
			capture.stepsBatches = append(capture.stepsBatches, batch)
			capture.mu.Unlock()
			writeStepsResponse(w, batch, repeatStatus("ACCEPTED", len(batch.Events)), len(batch.Events), 0, 0)
		case "/api/heart-rate/summary":
			capture.mu.Lock()
			capture.summaryHits++
			capture.mu.Unlock()
			http.NotFound(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
}

func TestUnifiedRunnerMetricSelectorsReuseSingleSessionAndRetireSummary(t *testing.T) {
	tests := []struct {
		name          string
		selector      string
		wantHeartRate bool
		wantSteps     bool
	}{
		{name: "default both", wantHeartRate: true, wantSteps: true},
		{name: "heart-rate only", selector: "heart-rate", wantHeartRate: true},
		{name: "steps only", selector: "step", wantSteps: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			capture := &runCapture{}
			server := newRunServer(t, capture, 0)
			defer server.Close()
			args := []string{}
			if tt.selector != "" {
				args = append(args, tt.selector)
			}
			args = append(args, "--heart-rate-interval", "1s", "--steps-aggregation-interval", "1s", "--batch-interval", "1s", "--duration", "1300ms")
			plan, password, err := ParseRunPlan(args, testRunEnv(server.URL))
			if err != nil {
				t.Fatal(err)
			}
			if err := runPlan(context.Background(), plan, password, log.New(io.Discard, "", 0)); err != nil {
				t.Fatalf("runPlan() error = %v", err)
			}
			capture.mu.Lock()
			defer capture.mu.Unlock()
			if len(capture.loginCounts) != 1 || capture.loginCounts["loadtest-0001@vitaloom.test"] != 1 {
				t.Errorf("login counts=%+v; want one warmed session", capture.loginCounts)
			}
			if (len(capture.hrBatches) > 0) != tt.wantHeartRate || (len(capture.stepsBatches) > 0) != tt.wantSteps {
				t.Errorf("metric requests Heart Rate=%d Steps=%d", len(capture.hrBatches), len(capture.stepsBatches))
			}
			if capture.summaryHits != 0 {
				t.Errorf("legacy Heart Rate summary requested %d times", capture.summaryHits)
			}
		})
	}
}

func TestUnifiedRunnerUsesFixedOneSecondObservationsAndCommonVUs(t *testing.T) {
	capture := &runCapture{}
	server := newRunServer(t, capture, 0)
	defer server.Close()
	plan, password, err := ParseRunPlan([]string{"--virtual-users", "2", "--ramp-up", "0s", "--batch-size", "1", "--heart-rate-interval", "1s", "--steps-aggregation-interval", "1s", "--batch-interval", "1s", "--duration", "1300ms"}, testRunEnv(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	if err := runPlan(context.Background(), plan, password, log.New(io.Discard, "", 0)); err != nil {
		t.Fatalf("runPlan() error = %v", err)
	}
	capture.mu.Lock()
	defer capture.mu.Unlock()
	if len(capture.loginCounts) != 2 || capture.loginCounts["loadtest-0001@vitaloom.test"] != 1 || capture.loginCounts["loadtest-0002@vitaloom.test"] != 1 {
		t.Fatalf("account sessions were not warmed once: %+v", capture.loginCounts)
	}
	if len(capture.hrBatches) < 2 || len(capture.stepsBatches) < 2 {
		t.Fatalf("expected both metrics for both VUs; HR=%d Steps=%d", len(capture.hrBatches), len(capture.stepsBatches))
	}
	for _, batch := range capture.hrBatches {
		if len(batch.Events) > 1 {
			t.Errorf("Heart Rate batch exceeded cap 1: %d", len(batch.Events))
		}
	}
	for _, batch := range capture.stepsBatches {
		if len(batch.Events) > 1 {
			t.Errorf("Steps batch exceeded cap 1: %d", len(batch.Events))
		}
	}
}

func TestFailedWorkerDoesNotCancelSibling(t *testing.T) {
	var siblingHeartBatches atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/auth/login":
			var request authRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("decode login: %v", err)
			}
			_ = json.NewEncoder(w).Encode(authResponse{AccessToken: "access-" + request.Email, RefreshToken: "refresh-" + request.Email, ExpiresInSeconds: 900})
		case "/api/heart-rate/batch":
			var batch authenticatedHeartRateBatch
			if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
				t.Errorf("decode Heart Rate batch: %v", err)
			}
			if r.Header.Get("Authorization") == "Bearer access-loadtest-0001@vitaloom.test" {
				http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
				return
			}
			siblingHeartBatches.Add(1)
			writeHeartRateResponse(w, batch, repeatStatus("ACCEPTED", len(batch.Events)), len(batch.Events), 0, 0)
		case "/api/steps/batch":
			var batch authenticatedStepsBatch
			if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
				t.Errorf("decode Steps batch: %v", err)
			}
			writeStepsResponse(w, batch, repeatStatus("ACCEPTED", len(batch.Events)), len(batch.Events), 0, 0)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	plan, password, err := ParseRunPlan([]string{"--virtual-users", "2", "--batch-size", "1", "--heart-rate-interval", "1s", "--steps-aggregation-interval", "1s", "--batch-interval", "1s", "--duration", "2400ms", "--timeout", "100ms", "--max-retries", "0"}, testRunEnv(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	var output strings.Builder
	logger := log.New(&output, "", 0)
	started := time.Now()
	err = runPlan(context.Background(), plan, password, logger)
	if err == nil || !strings.Contains(err.Error(), "Heart Rate batch failed after bounded retries") {
		t.Fatalf("runPlan() error = %v; want exhausted worker error", err)
	}
	if elapsed := time.Since(started); elapsed < 2200*time.Millisecond {
		t.Fatalf("runPlan() ended after %s; sibling should run through planned duration", elapsed)
	}
	if got := siblingHeartBatches.Load(); got < 2 {
		t.Fatalf("sibling Heart Rate batches=%d; want ongoing sends through planned duration", got)
	}
	logs := output.String()
	if !strings.Contains(logs, "producer_worker_stopped run_id=") || !strings.Contains(logs, "cause=bounded_retries_exhausted") || !strings.Contains(logs, "cause=duration_elapsed") {
		t.Fatalf("worker lifecycle logs missing structured stop causes:\n%s", logs)
	}
	for _, line := range strings.Split(logs, "\n") {
		if !strings.Contains(line, "producer_worker_started") && !strings.Contains(line, "producer_worker_stopped") {
			continue
		}
		fields := strings.Fields(line)
		values := make(map[string]string)
		for _, field := range fields[1:] {
			parts := strings.SplitN(field, "=", 2)
			if len(parts) == 2 {
				values[parts[0]] = parts[1]
			}
		}
		keys := []string{"started_at"}
		if strings.Contains(line, "producer_worker_stopped") {
			keys = append(keys, "stopped_at", "completed_at")
		}
		for _, key := range keys {
			if _, parseErr := time.Parse(time.RFC3339Nano, values[key]); parseErr != nil {
				t.Errorf("invalid %s timestamp in lifecycle log: %q", key, values[key])
			}
		}
	}
}

func TestWorkerCanceledDuringRampIsLoggedAsNotStarted(t *testing.T) {
	var logins atomic.Int32
	loginsReady := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/auth/login" {
			http.NotFound(w, r)
			return
		}
		var request authRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode login: %v", err)
		}
		if logins.Add(1) == 2 {
			close(loginsReady)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(authResponse{AccessToken: "access-" + request.Email, RefreshToken: "refresh-" + request.Email, ExpiresInSeconds: 900})
	}))
	defer server.Close()
	plan, password, err := ParseRunPlan([]string{"--virtual-users", "2", "--ramp-up", "5s", "--batch-interval", "1s"}, testRunEnv(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var output strings.Builder
	workerStarted := make(chan struct{}, 1)
	writer := runLogWriter(func(p []byte) (int, error) {
		_, _ = output.Write(p)
		if strings.Contains(string(p), "producer_worker_started") && strings.Contains(string(p), "account_index=1") {
			select {
			case workerStarted <- struct{}{}:
			default:
			}
		}
		return len(p), nil
	})
	result := make(chan error, 1)
	go func() { result <- runPlan(ctx, plan, password, log.New(writer, "", 0)) }()
	select {
	case <-loginsReady:
	case <-time.After(2 * time.Second):
		t.Fatal("workers did not warm both sessions")
	}
	select {
	case <-workerStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("first worker did not start after session warmup")
	}
	cancel()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("runPlan() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runPlan() did not stop after external cancellation")
	}
	logs := output.String()
	if !strings.Contains(logs, "producer_worker_not_started run_id=") || !strings.Contains(logs, "account_index=2 cause=external_cancellation") {
		t.Fatalf("ramp-canceled worker was not reported as not started:\n%s", logs)
	}
	if strings.Contains(logs, "producer_worker_started run_id=") && strings.Contains(logs, "account_index=2 started_at=") {
		t.Fatalf("ramp-canceled worker was also reported as started:\n%s", logs)
	}
}

func TestSharedSessionRefreshIsSerializedAcrossMetricSenders(t *testing.T) {
	var refreshes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/auth/refresh" {
			http.NotFound(w, r)
			return
		}
		refreshes.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"accessToken":"fresh-access","expiresInSeconds":900}`)
	}))
	defer server.Close()
	auth := NewAuthClient(Config{APIBaseURL: server.URL, Timeout: time.Second}, "test-password-123", log.New(io.Discard, "", 0))
	shared := &sessionToken{auth: auth, session: &Session{AccessToken: "expired", RefreshToken: "refresh-token", AccessExpiresAt: time.Now().Add(-time.Second)}}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			token, err := shared.accessToken(context.Background())
			if err != nil || token != "fresh-access" {
				t.Errorf("accessToken()=%q err=%v", token, err)
			}
		}()
	}
	wg.Wait()
	if refreshes.Load() != 1 {
		t.Fatalf("refresh calls=%d; want one serialized refresh", refreshes.Load())
	}
}

func TestUnifiedRunnerDrainsQueuesOnShutdown(t *testing.T) {
	capture := &runCapture{}
	server := newRunServer(t, capture, 0)
	defer server.Close()
	plan, password, err := ParseRunPlan([]string{"--heart-rate-interval", "1s", "--steps-aggregation-interval", "1s", "--batch-interval", "1m"}, testRunEnv(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- runPlan(ctx, plan, password, log.New(io.Discard, "", 0)) }()
	time.Sleep(observationInterval + 100*time.Millisecond)
	cancel()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("runPlan() shutdown error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runPlan() did not drain and stop within the bound")
	}
	capture.mu.Lock()
	defer capture.mu.Unlock()
	if len(capture.hrBatches) != 1 || len(capture.stepsBatches) != 1 {
		t.Fatalf("shutdown did not send queued events: HR=%d Steps=%d", len(capture.hrBatches), len(capture.stepsBatches))
	}
}

func TestShortRunDrainsObservedStepsPrefixWithoutFutureTimestamps(t *testing.T) {
	capture := &runCapture{}
	server := newRunServer(t, capture, 0)
	defer server.Close()
	plan, password, err := ParseRunPlan([]string{"step", "--steps-aggregation-interval", "60s", "--sync-interval", "1m", "--duration", "1250ms"}, testRunEnv(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	if err := runPlan(context.Background(), plan, password, log.New(io.Discard, "", 0)); err != nil {
		t.Fatal(err)
	}
	capture.mu.Lock()
	defer capture.mu.Unlock()
	if len(capture.stepsBatches) != 1 || len(capture.stepsBatches[0].Events) != 1 {
		t.Fatalf("short run did not flush one observed Steps prefix: %+v", capture.stepsBatches)
	}
	event := capture.stepsBatches[0].Events[0]
	if event.OccurredAt != nil || event.StartAt == nil || event.EndAt == nil || event.EndAt.After(time.Now().UTC()) {
		t.Fatalf("invalid/future Steps interval: %+v", event)
	}
	duration := event.EndAt.Sub(*event.StartAt)
	if duration < time.Second || duration > 2*time.Second || event.StepsDelta < 1 || event.StepsDelta > 5*int(duration/time.Second) {
		t.Fatalf("invalid short-run aggregate: duration=%s event=%+v", duration, event)
	}
}

func TestBatchChunkHelpersPreserveOrderAndCap(t *testing.T) {
	hrQueue := make([]HeartRateEvent, 7)
	stepsQueue := make([]StepEvent, 7)
	for i := range hrQueue {
		hrQueue[i].EventID = fmt.Sprintf("hr-%d", i)
		stepsQueue[i].EventID = fmt.Sprintf("steps-%d", i)
	}
	hrFirst, hrRemainder := takeHeartRateChunk(hrQueue, 3)
	stepsFirst, stepsRemainder := takeStepsChunk(stepsQueue, 3)
	if len(hrFirst) != 3 || len(hrRemainder) != 4 || len(stepsFirst) != 3 || len(stepsRemainder) != 4 || hrFirst[2].EventID != "hr-2" || stepsRemainder[0].EventID != "steps-3" {
		t.Fatalf("chunks lost order/cap: HR %d+%d Steps %d+%d", len(hrFirst), len(hrRemainder), len(stepsFirst), len(stepsRemainder))
	}
}

func TestHeartRateObservationsStayOneSecondApart(t *testing.T) {
	capture := &runCapture{}
	server := newRunServer(t, capture, 0)
	defer server.Close()
	plan, password, err := ParseRunPlan([]string{"heart-rate", "--heart-rate-interval", "1s", "--batch-interval", "2s", "--duration", "2300ms"}, testRunEnv(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	if err := runPlan(context.Background(), plan, password, log.New(io.Discard, "", 0)); err != nil {
		t.Fatal(err)
	}
	capture.mu.Lock()
	defer capture.mu.Unlock()
	var events []HeartRateEvent
	for _, batch := range capture.hrBatches {
		events = append(events, batch.Events...)
	}
	if len(events) < 2 {
		t.Fatalf("captured %d Heart Rate events; want at least 2", len(events))
	}
	for i := 1; i < len(events); i++ {
		gap := events[i].OccurredAt.Sub(events[i-1].OccurredAt)
		if gap < 999*time.Millisecond || gap > 1001*time.Millisecond {
			t.Fatalf("measurement gap %s; want approximately 1s", gap)
		}
	}
}

func TestSyncStaggerSpreadsVirtualUsersAcrossBoundedPhaseSlots(t *testing.T) {
	const users = 1000
	total := time.Minute
	const runID = "run-phase-test"
	first := syncPhase(total, 0, users, runID)
	last := syncPhase(total, users-1, users, runID)
	if first <= 0 || last <= first || last >= total {
		t.Fatalf("sync phases out of bounds: first=%s last=%s total=%s", first, last, total)
	}
	repeated := syncPhase(total, 0, users, runID)
	if repeated != first {
		t.Fatalf("same run and VU produced unstable sync phase: first=%s repeated=%s", first, repeated)
	}
	for index := 1; index < users; index++ {
		slotStart := spreadDuration(total, index, users)
		slotEnd := spreadDuration(total, index+1, users)
		phase := syncPhase(total, index, users, runID)
		if phase < slotStart+(slotEnd-slotStart)/4 || phase > slotStart+3*(slotEnd-slotStart)/4 {
			t.Fatalf("VU %d phase %s falls outside bounded jitter range for slot [%s,%s)", index+1, phase, slotStart, slotEnd)
		}
		previous := syncPhase(total, index-1, users, runID)
		if phase <= previous {
			t.Fatalf("sync phases are not staggered at VU %d: previous=%s phase=%s", index+1, previous, phase)
		}
	}
}

func repeatStatus(status string, count int) []string {
	statuses := make([]string, count)
	for i := range statuses {
		statuses[i] = status
	}
	return statuses
}
