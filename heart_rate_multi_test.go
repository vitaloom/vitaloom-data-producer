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
	"testing"
	"time"
)

func TestHeartRateMultiUserWarmupIsolationAndRunIdentity(t *testing.T) {
	var mu sync.Mutex
	loggedIn := map[string]bool{}
	maxLogins, activeLogins := 0, 0
	eventIDs := map[string]bool{}
	deviceTokens := map[string]string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/auth/login":
			var body authRequest
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("login body: %v", err)
				return
			}
			mu.Lock()
			activeLogins++
			if activeLogins > maxLogins {
				maxLogins = activeLogins
			}
			mu.Unlock()
			time.Sleep(2 * time.Millisecond)
			mu.Lock()
			loggedIn[body.Email] = true
			activeLogins--
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(authResponse{AccessToken: "access-" + body.Email, RefreshToken: "refresh-" + body.Email, ExpiresInSeconds: 900})
		case "/api/heart-rate/batch":
			token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			var batch authenticatedHeartRateBatch
			if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
				t.Errorf("batch body: %v", err)
				return
			}
			mu.Lock()
			if len(loggedIn) != 2 {
				t.Errorf("measurement started before warm-up completed: logins=%d", len(loggedIn))
			}
			if old, exists := deviceTokens[batch.DeviceID]; exists && old != token {
				t.Errorf("device %s changed account token", batch.DeviceID)
			}
			deviceTokens[batch.DeviceID] = token
			for _, event := range batch.Events {
				if eventIDs[event.EventID] {
					t.Errorf("duplicate event id %s", event.EventID)
				}
				eventIDs[event.EventID] = true
			}
			mu.Unlock()
			results := make([]heartRateItemResult, len(batch.Events))
			for i, event := range batch.Events {
				results[i] = heartRateItemResult{Index: i, EventID: event.EventID, Status: "ACCEPTED"}
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(authenticatedHeartRateResponse{RunID: batch.RunID, Received: len(batch.Events), Accepted: len(batch.Events), Results: results})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	cfg := Config{APIBaseURL: server.URL, Timeout: time.Second, MaxRetries: 0}
	options := heartRateMultiOptions{virtualUsers: 2, devicesPerUser: 1, measurementInterval: 5 * time.Millisecond, batchInterval: 20 * time.Millisecond, rampUp: 20 * time.Millisecond, duration: 100 * time.Millisecond}
	if err := runHeartRateMultiUser(context.Background(), cfg, "test-password", options, log.New(io.Discard, "", 0)); err != nil {
		t.Fatalf("runHeartRateMultiUser() error = %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if maxLogins != 1 {
		t.Errorf("max concurrent logins = %d; want serial warm-up", maxLogins)
	}
	if len(deviceTokens) != 2 || deviceTokens["device-loadtest-0001"] != "access-loadtest-0001@vitaloom.test" || deviceTokens["device-loadtest-0002"] != "access-loadtest-0002@vitaloom.test" {
		t.Errorf("device/token identity mapping = %+v", deviceTokens)
	}
	if len(eventIDs) == 0 {
		t.Error("scenario sent no events")
	}
}

func TestHeartRateMultiUserStaggerMathAndFlags(t *testing.T) {
	if got := spreadDuration(10*time.Second, 9, 10); got != 9*time.Second {
		t.Fatalf("last ramp offset=%s; want 9s", got)
	}
	if got := spreadDuration(5*time.Second, 9, 10); got != 4500*time.Millisecond {
		t.Fatalf("last flush offset=%s; want 4.5s", got)
	}
	env := func(key string) string {
		if key == "VITALOOM_API_BASE_URL" {
			return "http://nginx.test"
		}
		if key == "VITALOOM_TEST_PASSWORD" {
			return "test-password-123"
		}
		return ""
	}
	for _, args := range [][]string{
		{"--virtual-users", "0", "--duration", "1s"}, {"--virtual-users", "1001", "--duration", "1s"},
		{"--virtual-users", "1", "--devices-per-user", "2", "--duration", "1s"},
		{"--virtual-users", "1", "--account", "1", "--duration", "1s"},
		{"--virtual-users", "1", "--duration", "1s", "--ramp-up", "1s"},
	} {
		if _, _, _, err := parseHeartRateMultiUserCommand(args, env); err == nil {
			t.Errorf("invalid args accepted: %v", args)
		}
	}
	options, cfg, password, err := parseHeartRateMultiUserCommand([]string{"--virtual-users", "10", "--ramp-up", "5s", "--duration", "20s", "--measurement-interval", "1s", "--batch-interval", "5s"}, env)
	if err != nil || options.virtualUsers != 10 || options.rampUp != 5*time.Second || options.duration != 20*time.Second || cfg.APIBaseURL != "http://nginx.test" || password != "test-password-123" {
		t.Fatalf("valid parse = %+v, %+v, %q, %v", options, cfg, password, err)
	}
	if strings.Contains(fmt.Sprint(options), "password") {
		t.Fatal("scenario options unexpectedly contain credentials")
	}
}
