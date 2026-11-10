package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const (
	maxAuthRetries = 2
	maxRetryWait   = 30 * time.Second
)

type Account struct {
	Index int
	Email string
}

type Session struct {
	Email           string
	AccessToken     string
	RefreshToken    string
	AccessExpiresAt time.Time
}

type AuthStatusError struct {
	StatusCode int
	Code       string
}

func (e *AuthStatusError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("authentication request returned HTTP %d (%s)", e.StatusCode, e.Code)
	}
	return fmt.Sprintf("authentication request returned HTTP %d", e.StatusCode)
}

type AuthClient struct {
	baseURL    string
	httpClient *http.Client
	password   string
	maxRetries int
	retryDelay time.Duration
	logger     *log.Logger
	rateLimits atomic.Uint64
}

func NewAuthClient(cfg Config, password string, logger *log.Logger) *AuthClient {
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	retries := cfg.MaxRetries
	if retries > maxAuthRetries {
		retries = maxAuthRetries
	}
	if retries < 0 {
		retries = 0
	}
	return &AuthClient{
		baseURL:    strings.TrimRight(cfg.APIBaseURL, "/"),
		httpClient: newAuthHTTPClient(cfg.Timeout),
		password:   password,
		maxRetries: retries,
		retryDelay: cfg.RetryDelay,
		logger:     logger,
	}
}

func newAuthHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func (c *AuthClient) RateLimitCount() uint64 { return c.rateLimits.Load() }

func deterministicAccount(index int) Account {
	return Account{Index: index, Email: fmt.Sprintf("loadtest-%04d@vitaloom.test", index)}
}

func (c *AuthClient) Provision(ctx context.Context, from, to int) error {
	if err := validateAccountRange(from, to); err != nil {
		return err
	}
	for index := from; index <= to; index++ {
		account := deterministicAccount(index)
		var registered authResponse
		err := c.post(ctx, "/api/auth/register", http.StatusCreated, authRequest{Email: account.Email, Password: c.password}, &registered)
		if err == nil {
			if registered.AuthStatus == "LOGIN_REQUIRED" {
				if _, loginErr := c.Login(ctx, account.Email); loginErr != nil {
					return accountFailure(account, loginErr)
				}
			}
			continue
		}
		var statusErr *AuthStatusError
		if errors.As(err, &statusErr) && statusErr.StatusCode == http.StatusConflict && statusErr.Code == "EMAIL_ALREADY_REGISTERED" {
			if _, loginErr := c.Login(ctx, account.Email); loginErr != nil {
				return accountFailure(account, loginErr)
			}
			continue
		}
		return accountFailure(account, err)
	}
	return nil
}

func (c *AuthClient) WarmSessions(ctx context.Context, from, to int) (map[string]*Session, error) {
	if err := validateAccountRange(from, to); err != nil {
		return nil, err
	}
	sessions := make(map[string]*Session, to-from+1)
	for index := from; index <= to; index++ {
		account := deterministicAccount(index)
		session, err := c.Login(ctx, account.Email)
		if err != nil {
			return nil, accountFailure(account, err)
		}
		sessions[account.Email] = session
	}
	return sessions, nil
}

func (c *AuthClient) Login(ctx context.Context, email string) (*Session, error) {
	var result authResponse
	if err := c.post(ctx, "/api/auth/login", http.StatusOK, authRequest{Email: email, Password: c.password}, &result); err != nil {
		return nil, err
	}
	if result.AccessToken == "" || result.RefreshToken == "" || result.ExpiresInSeconds <= 0 {
		return nil, errors.New("authentication response was incomplete")
	}
	return &Session{
		Email:           email,
		AccessToken:     result.AccessToken,
		RefreshToken:    result.RefreshToken,
		AccessExpiresAt: time.Now().Add(time.Duration(result.ExpiresInSeconds) * time.Second),
	}, nil
}

func (c *AuthClient) Refresh(ctx context.Context, session *Session) error {
	if session == nil || session.RefreshToken == "" {
		return errors.New("session has no refresh token")
	}
	refreshToken := session.RefreshToken
	var result refreshResponse
	if err := c.post(ctx, "/api/auth/refresh", http.StatusOK, refreshRequest{RefreshToken: refreshToken}, &result); err != nil {
		return err
	}
	if result.AccessToken == "" || result.ExpiresInSeconds <= 0 {
		return errors.New("refresh response was incomplete")
	}
	session.AccessToken = result.AccessToken
	session.RefreshToken = refreshToken
	session.AccessExpiresAt = time.Now().Add(time.Duration(result.ExpiresInSeconds) * time.Second)
	return nil
}

func (c *AuthClient) post(ctx context.Context, endpoint string, expectedStatus int, payload, response any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return errors.New("could not encode authentication request")
	}
	maxAttempts := c.maxRetries + 1
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+endpoint, bytes.NewReader(body))
		if err != nil {
			return errors.New("could not prepare authentication request")
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Accept", "application/json")
		result, doErr := c.httpClient.Do(request)
		if doErr != nil {
			if !isRetryableNetworkError(ctx, doErr) || attempt == maxAttempts {
				return safeNetworkError(ctx, doErr)
			}
			if err := c.wait(ctx, c.retryDelay, endpoint, attempt); err != nil {
				return err
			}
			continue
		}
		if result.StatusCode == http.StatusTooManyRequests {
			count := c.rateLimits.Add(1)
			c.logger.Printf("auth_rate_limited endpoint=%s count=%d", endpoint, count)
		}
		responseBody, readErr := io.ReadAll(io.LimitReader(result.Body, maxResponseBodyBytes+1))
		_ = result.Body.Close()
		if readErr != nil {
			if attempt < maxAttempts {
				delay := authRetryDelay(result.Header.Get("Retry-After"), c.retryDelay)
				if err := c.wait(ctx, delay, endpoint, attempt); err != nil {
					return err
				}
				continue
			}
			return errors.New("could not read authentication response")
		}
		if len(responseBody) > maxResponseBodyBytes {
			return errors.New("authentication response exceeded the size limit")
		}
		if result.StatusCode != expectedStatus {
			statusErr := authStatusError(result.StatusCode, responseBody)
			retryable := result.StatusCode == http.StatusRequestTimeout || result.StatusCode == http.StatusTooManyRequests || result.StatusCode >= http.StatusInternalServerError
			if !retryable || attempt == maxAttempts {
				return statusErr
			}
			delay := authRetryDelay(result.Header.Get("Retry-After"), c.retryDelay)
			if err := c.wait(ctx, delay, endpoint, attempt); err != nil {
				return err
			}
			continue
		}
		if err := json.Unmarshal(responseBody, response); err != nil {
			return errors.New("authentication response was invalid")
		}
		return nil
	}
	return errors.New("authentication request failed")
}

func authStatusError(status int, body []byte) *AuthStatusError {
	var envelope struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	_ = json.Unmarshal(body, &envelope)
	code := envelope.Error.Code
	switch code {
	case "EMAIL_ALREADY_REGISTERED", "INVALID_CREDENTIALS", "INVALID_REFRESH_TOKEN", "VALIDATION_ERROR":
		return &AuthStatusError{StatusCode: status, Code: code}
	default:
		return &AuthStatusError{StatusCode: status}
	}
}

func (c *AuthClient) wait(ctx context.Context, delay time.Duration, endpoint string, attempt int) error {
	if delay > maxRetryWait {
		return fmt.Errorf("retry for %s requires a wait above the %s cap", endpoint, maxRetryWait)
	}
	c.logger.Printf("auth_retry endpoint=%s next_attempt=%d/%d delay=%s", endpoint, attempt+1, c.maxRetries+1, delay)
	if delay <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func parseRetryAfter(value string, now time.Time) (time.Duration, bool) {
	if seconds, err := strconv.Atoi(strings.TrimSpace(value)); err == nil {
		if seconds < 0 {
			return 0, false
		}
		if int64(seconds) > int64(maxRetryWait/time.Second) {
			return maxRetryWait + time.Second, true
		}
		return time.Duration(seconds) * time.Second, true
	}
	when, err := http.ParseTime(value)
	if err != nil {
		return 0, false
	}
	delay := when.Sub(now)
	if delay < 0 {
		delay = 0
	}
	return delay, true
}

func authRetryDelay(header string, fallback time.Duration) time.Duration {
	if parsed, ok := parseRetryAfter(header, time.Now()); ok {
		return parsed
	}
	return fallback
}

func isRetryableNetworkError(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return false
	}
	var urlErr *url.Error
	return errors.As(err, &urlErr)
}

func safeNetworkError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) && urlErr.Timeout() {
		return errors.New("authentication request timed out")
	}
	return errors.New("authentication network request failed")
}

type authRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type refreshRequest struct {
	RefreshToken string `json:"refreshToken"`
}

type authResponse struct {
	AccessToken      string `json:"accessToken"`
	RefreshToken     string `json:"refreshToken"`
	ExpiresInSeconds int64  `json:"expiresInSeconds"`
	AuthStatus       string `json:"authStatus"`
}

type refreshResponse struct {
	AccessToken      string `json:"accessToken"`
	ExpiresInSeconds int64  `json:"expiresInSeconds"`
}
