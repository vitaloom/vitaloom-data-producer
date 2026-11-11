package main

import (
	"context"
	"encoding/json"
	"errors"
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

func generatedTestPassword(t *testing.T) string {
	t.Helper()
	return strings.ReplaceAll(t.Name(), "/", "-") + "-generated-9!"
}

func authTestClient(serverURL, password string, retries int, delay time.Duration, logger *log.Logger) *AuthClient {
	return NewAuthClient(Config{APIBaseURL: serverURL, Timeout: time.Second, MaxRetries: retries, RetryDelay: delay}, password, logger)
}

func TestProvisionCreatesAndVerifiesExistingAccounts(t *testing.T) {
	password := generatedTestPassword(t)
	accounts := map[string]string{}
	var mu sync.Mutex
	var registrations, logins int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body authRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode request: %v", err)
			http.Error(w, "invalid", http.StatusBadRequest)
			return
		}
		if body.Password != password {
			t.Errorf("request password did not match generated test value")
		}
		mu.Lock()
		defer mu.Unlock()
		switch r.URL.Path {
		case "/api/auth/register":
			registrations++
			if _, exists := accounts[body.Email]; exists {
				w.Header().Set("Content-Type", "application/problem+json")
				w.WriteHeader(http.StatusConflict)
				_, _ = io.WriteString(w, `{"type":"about:blank","title":"Conflict","status":409,"detail":"An account with this email already exists.","instance":"/api/auth/register","code":"EMAIL_ALREADY_REGISTERED"}`)
				return
			}
			accounts[body.Email] = body.Password
			if body.Email == "loadtest-0001@vitaloom.test" && registrations == 1 {
				// Model a server-side create whose response is lost to the caller.
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(authResponse{AccessToken: "ignored-access", RefreshToken: "ignored-refresh", ExpiresInSeconds: 900})
		case "/api/auth/login":
			logins++
			if accounts[body.Email] != body.Password {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = io.WriteString(w, `{"error":{"code":"INVALID_CREDENTIALS"}}`)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(authResponse{AccessToken: "access-" + body.Email, RefreshToken: "refresh-" + body.Email, ExpiresInSeconds: 900})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := authTestClient(server.URL, password, 1, 0, nil)
	if err := client.Provision(context.Background(), 1, 2); err != nil {
		t.Fatalf("initial provision: %v", err)
	}
	if err := client.Provision(context.Background(), 1, 2); err != nil {
		t.Fatalf("repeat provision: %v", err)
	}
	if registrations != 5 || logins != 3 {
		t.Fatalf("registrations=%d logins=%d; want 5 and 3 conflict verifications including retry after lost response", registrations, logins)
	}
}

func TestProvisionUsesLoginRequiredFallback(t *testing.T) {
	password := generatedTestPassword(t)
	var loginCount int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/auth/register" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(w, `{"user":{"id":"opaque-id","email":"loadtest-0001@vitaloom.test"},"authStatus":"LOGIN_REQUIRED"}`)
			return
		}
		loginCount++
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"accessToken":"fresh-access","refreshToken":"fresh-refresh","expiresInSeconds":900}`)
	}))
	defer server.Close()
	if err := authTestClient(server.URL, password, 0, 0, nil).Provision(context.Background(), 1, 1); err != nil {
		t.Fatalf("Provision() error = %v", err)
	}
	if loginCount != 1 {
		t.Fatalf("login count = %d; want 1", loginCount)
	}
}

func TestBadCredentialsErrorDoesNotLeakSecrets(t *testing.T) {
	password := generatedTestPassword(t)
	secretToken := "server-body-token-should-not-escape"
	var logs strings.Builder
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = fmt.Fprintf(w, `{"error":{"code":"INVALID_CREDENTIALS","message":"%s %s"}}`, password, secretToken)
	}))
	defer server.Close()
	client := authTestClient(server.URL, password, 0, 0, log.New(&logs, "", 0))
	_, err := client.Login(context.Background(), "loadtest-0001@vitaloom.test")
	if err == nil || !strings.Contains(err.Error(), "INVALID_CREDENTIALS") {
		t.Fatalf("Login() error = %v; want safe invalid credentials status", err)
	}
	if strings.Contains(err.Error(), password) || strings.Contains(err.Error(), secretToken) || strings.Contains(logs.String(), password) || strings.Contains(logs.String(), secretToken) {
		t.Fatalf("auth failure leaked secret in error/log: error=%q logs=%q", err, logs.String())
	}
}

func TestWarmSessionsArePerAccountAndRefreshRetainsRefreshToken(t *testing.T) {
	password := generatedTestPassword(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/auth/login":
			var body authRequest
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode login: %v", err)
			}
			_ = json.NewEncoder(w).Encode(authResponse{AccessToken: "access-" + body.Email, RefreshToken: "refresh-" + body.Email, ExpiresInSeconds: 120})
		case "/api/auth/refresh":
			var body refreshRequest
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode refresh: %v", err)
			}
			if body.RefreshToken != "refresh-loadtest-0001@vitaloom.test" {
				t.Errorf("refresh token = %q; wrong account token", body.RefreshToken)
			}
			_ = json.NewEncoder(w).Encode(refreshResponse{AccessToken: "renewed-access", ExpiresInSeconds: 180})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := authTestClient(server.URL, password, 0, 0, nil)
	sessions, err := client.WarmSessions(context.Background(), 1, 2)
	if err != nil {
		t.Fatalf("WarmSessions() error = %v", err)
	}
	first := sessions["loadtest-0001@vitaloom.test"]
	second := sessions["loadtest-0002@vitaloom.test"]
	if first == nil || second == nil || first == second {
		t.Fatalf("sessions are missing or share storage: first=%p second=%p", first, second)
	}
	if first.AccessToken == second.AccessToken || first.RefreshToken == second.RefreshToken {
		t.Fatalf("accounts share tokens: first=%+v second=%+v", first, second)
	}
	oldRefresh := first.RefreshToken
	if err := client.Refresh(context.Background(), first); err != nil {
		t.Fatalf("Refresh() error = %v", err)
	}
	if first.AccessToken != "renewed-access" || first.RefreshToken != oldRefresh || time.Until(first.AccessExpiresAt) <= 0 {
		t.Fatalf("refreshed session = %+v; expected new access and retained refresh token", first)
	}
}

func TestAuthRetries429WithRetryAfterAndCountsSeparately(t *testing.T) {
	password := generatedTestPassword(t)
	var requests int
	var logs strings.Builder
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if requests == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = io.WriteString(w, `{"error":{"message":"body must not be logged"}}`)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"accessToken":"access","refreshToken":"refresh","expiresInSeconds":900}`)
	}))
	defer server.Close()
	client := authTestClient(server.URL, password, 10, 0, log.New(&logs, "", 0))
	if _, err := client.Login(context.Background(), "loadtest-0001@vitaloom.test"); err != nil {
		t.Fatalf("Login() error = %v", err)
	}
	if requests != 2 || client.RateLimitCount() != 1 {
		t.Fatalf("requests=%d rateLimited=%d; want 2 and 1", requests, client.RateLimitCount())
	}
	if !strings.Contains(logs.String(), "auth_rate_limited") || strings.Contains(logs.String(), "body must not be logged") {
		t.Fatalf("logs do not safely identify 429: %q", logs.String())
	}
}

func TestAuthStopsWhenRetryAfterExceedsCap(t *testing.T) {
	password := generatedTestPassword(t)
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.Header().Set("Retry-After", "31")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()
	client := authTestClient(server.URL, password, 2, 0, nil)
	_, err := client.Login(context.Background(), "loadtest-0001@vitaloom.test")
	if err == nil || !strings.Contains(err.Error(), "above the 30s cap") {
		t.Fatalf("Login() error = %v; want bounded Retry-After failure", err)
	}
	if requests != 1 {
		t.Fatalf("requests = %d; expected retry to abort before second request", requests)
	}
}

func TestParseRetryAfterSupportsHTTPDate(t *testing.T) {
	now := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	want := now.Add(5 * time.Second)
	got, ok := parseRetryAfter(want.Format(http.TimeFormat), now)
	if !ok || got != 5*time.Second {
		t.Fatalf("parseRetryAfter() = %s, %v; want 5s, true", got, ok)
	}
}

func TestAuthConfigRequiresExplicitNginxURLAndPassword(t *testing.T) {
	password := generatedTestPassword(t)
	for name, env := range map[string]map[string]string{
		"missing URL":     {"VITALOOM_TEST_PASSWORD": password},
		"short password":  {"VITALOOM_API_BASE_URL": "http://nginx.test", "VITALOOM_TEST_PASSWORD": password[:5]},
		"URL credentials": {"VITALOOM_API_BASE_URL": "http://user:secret@nginx.test", "VITALOOM_TEST_PASSWORD": password},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := authConfigFromEnv(func(key string) string { return env[key] })
			if err == nil {
				t.Fatal("authConfigFromEnv() unexpectedly succeeded")
			}
			if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), password) {
				t.Fatalf("configuration error leaked credentials: %v", err)
			}
		})
	}
}

func TestParseProvisionArgsRejectsBoundsAndStrayArguments(t *testing.T) {
	for _, args := range [][]string{{"--from", "0"}, {"--to", "1001"}, {"--from", "2", "--to", "1"}, {"stray"}} {
		if _, _, err := parseProvisionArgs(args); err == nil {
			t.Errorf("parseProvisionArgs(%v) unexpectedly succeeded", args)
		}
	}
	from, to, err := parseProvisionArgs(nil)
	if err != nil || from != 1 || to != 1000 {
		t.Fatalf("parseProvisionArgs(nil) = %d, %d, %v; want 1, 1000, nil", from, to, err)
	}
}

func TestAuthDoesNotFollowRedirects(t *testing.T) {
	password := generatedTestPassword(t)
	var targetRequests int
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { targetRequests++ }))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/api/auth/login", http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	_, err := authTestClient(redirect.URL, password, 0, 0, nil).Login(context.Background(), "loadtest-0001@vitaloom.test")
	var statusErr *AuthStatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("Login() error = %v; expected redirect status without following", err)
	}
	if targetRequests != 0 {
		t.Fatalf("cross-origin target received %d requests", targetRequests)
	}
}
