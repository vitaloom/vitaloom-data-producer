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
	"strings"
	"time"
)

const maxResponseBodyBytes = 64 * 1024

type BatchIngestionResponse struct {
	RunID string `json:"runId"`
	Received int `json:"received"`
	Accepted int `json:"accepted"`
	Duplicates int `json:"duplicates"`
}
type SendResult struct { StatusCode int; Attempts int; Response BatchIngestionResponse }
type APIClient struct {
	baseURL string
	httpClient *http.Client
	maxRetries int
	retryDelay time.Duration
	logger *log.Logger
}
func NewAPIClient(cfg Config, logger *log.Logger) *APIClient {
	if logger == nil { logger = log.New(io.Discard, "", 0) }
	return &APIClient{baseURL: strings.TrimRight(cfg.APIBaseURL, "/"), httpClient: &http.Client{Timeout: cfg.Timeout}, maxRetries: cfg.MaxRetries, retryDelay: cfg.RetryDelay, logger: logger}
}
func (c *APIClient) SendHeartRateBatch(ctx context.Context, batch HeartRateBatch) (SendResult, error) {
	return c.sendBatch(ctx, "/api/heart-rate/batch", "heart-rate", batch.RunID, len(batch.Events), batch)
}
func (c *APIClient) SendStepBatch(ctx context.Context, batch StepBatch) (SendResult, error) {
	return c.sendBatch(ctx, "/api/steps/batch", "steps", batch.RunID, len(batch.Events), batch)
}
func (c *APIClient) sendBatch(ctx context.Context, endpoint, metric, runID string, eventCount int, payload any) (SendResult, error) {
	body, err := json.Marshal(payload)
	if err != nil { return SendResult{}, fmt.Errorf("encode %s batch: %w", metric, err) }
	maxAttempts := c.maxRetries + 1
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		// Finish in-flight batches and bounded retries before stopping on Ctrl+C.
		request, requestErr := http.NewRequestWithContext(context.WithoutCancel(ctx), http.MethodPost, c.baseURL+endpoint, bytes.NewReader(body))
		if requestErr != nil { return SendResult{Attempts: attempt}, fmt.Errorf("build %s request: %w", metric, requestErr) }
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Accept", "application/json")
		started := time.Now()
		response, doErr := c.httpClient.Do(request)
		if doErr != nil {
			lastErr = fmt.Errorf("POST %s: %w", endpoint, doErr)
			if attempt < maxAttempts { c.logRetry(endpoint, metric, attempt, maxAttempts, lastErr); _ = sleep(c.retryDelay); continue }
			return SendResult{Attempts: attempt}, lastErr
		}
		responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, maxResponseBodyBytes+1))
		closeErr := response.Body.Close()
		if readErr != nil {
			lastErr = fmt.Errorf("read %s response: %w", metric, readErr)
			if attempt < maxAttempts { c.logRetry(endpoint, metric, attempt, maxAttempts, lastErr); _ = sleep(c.retryDelay); continue }
			return SendResult{StatusCode: response.StatusCode, Attempts: attempt}, lastErr
		}
		if closeErr != nil { c.logger.Printf("warning: close response body for %s: %v", metric, closeErr) }
		if len(responseBody) > maxResponseBodyBytes { responseBody = append(responseBody[:maxResponseBodyBytes], []byte("...<truncated>")...) }
		if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
			message := strings.TrimSpace(string(responseBody))
			if message == "" { message = http.StatusText(response.StatusCode) }
			lastErr = fmt.Errorf("POST %s returned HTTP %d: %s", endpoint, response.StatusCode, message)
			retryable := response.StatusCode == http.StatusRequestTimeout || response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= http.StatusInternalServerError
			if retryable && attempt < maxAttempts { c.logRetry(endpoint, metric, attempt, maxAttempts, lastErr); _ = sleep(c.retryDelay); continue }
			return SendResult{StatusCode: response.StatusCode, Attempts: attempt}, lastErr
		}
		var result BatchIngestionResponse
		if err := json.Unmarshal(responseBody, &result); err != nil {
			lastErr = fmt.Errorf("decode %s response from HTTP %d: %w", metric, response.StatusCode, err)
			if attempt < maxAttempts { c.logRetry(endpoint, metric, attempt, maxAttempts, lastErr); _ = sleep(c.retryDelay); continue }
			return SendResult{StatusCode: response.StatusCode, Attempts: attempt}, lastErr
		}
		if result.RunID != runID || result.Received != eventCount || result.Accepted < 0 || result.Duplicates < 0 || result.Accepted+result.Duplicates != result.Received {
			lastErr = fmt.Errorf("unexpected %s response: runId=%q received=%d accepted=%d duplicates=%d; expected runId=%q received=%d and accepted+duplicates=received", metric, result.RunID, result.Received, result.Accepted, result.Duplicates, runID, eventCount)
			if attempt < maxAttempts { c.logRetry(endpoint, metric, attempt, maxAttempts, lastErr); _ = sleep(c.retryDelay); continue }
			return SendResult{StatusCode: response.StatusCode, Attempts: attempt}, lastErr
		}
		c.logger.Printf("batch metric=%s endpoint=%s status=%d run_id=%s received=%d accepted=%d duplicates=%d attempts=%d elapsed=%s", metric, endpoint, response.StatusCode, runID, result.Received, result.Accepted, result.Duplicates, attempt, time.Since(started).Round(time.Millisecond))
		return SendResult{StatusCode: response.StatusCode, Attempts: attempt, Response: result}, nil
	}
	if lastErr == nil { lastErr = errors.New("request failed without an error detail") }
	return SendResult{Attempts: maxAttempts}, lastErr
}
func (c *APIClient) logRetry(endpoint, metric string, attempt, maxAttempts int, err error) {
	c.logger.Printf("retry metric=%s endpoint=%s next_attempt=%d/%d reason=%v", metric, endpoint, attempt+1, maxAttempts, err)
}
func sleep(duration time.Duration) error {
	if duration <= 0 { return nil }
	timer := time.NewTimer(duration); defer timer.Stop(); <-timer.C
	return nil
}
