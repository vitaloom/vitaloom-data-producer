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
	"sync/atomic"
	"time"
)

type authenticatedHeartRateBatch struct {
	DeviceID string           `json:"deviceId"`
	RunID    string           `json:"runId"`
	Events   []HeartRateEvent `json:"events"`
}

type heartRateItemError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type heartRateItemResult struct {
	Index   int                 `json:"index"`
	EventID string              `json:"eventId"`
	Status  string              `json:"status"`
	Error   *heartRateItemError `json:"error,omitempty"`
}

type authenticatedHeartRateResponse struct {
	RunID      string                `json:"runId"`
	Received   int                   `json:"received"`
	Accepted   int                   `json:"accepted"`
	Duplicates int                   `json:"duplicates"`
	Rejected   int                   `json:"rejected"`
	Results    []heartRateItemResult `json:"results"`
}

type authenticatedHeartRateClient struct {
	baseURL    string
	httpClient *http.Client
	maxRetries int
	retryDelay time.Duration
	logger     *log.Logger
	rateLimits atomic.Int64
}

func newAuthenticatedHeartRateClient(cfg Config, logger *log.Logger) *authenticatedHeartRateClient {
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	return &authenticatedHeartRateClient{baseURL: strings.TrimRight(cfg.APIBaseURL, "/"), httpClient: newAuthHTTPClient(cfg.Timeout), maxRetries: cfg.MaxRetries, retryDelay: cfg.RetryDelay, logger: logger}
}

func (c *authenticatedHeartRateClient) sendBatch(ctx context.Context, token string, batch authenticatedHeartRateBatch) (authenticatedHeartRateResponse, int, error) {
	if len(batch.Events) < 1 || len(batch.Events) > maxBatchSize {
		return authenticatedHeartRateResponse{}, 0, fmt.Errorf("Heart Rate batch must contain between 1 and %d events", maxBatchSize)
	}
	body, err := json.Marshal(batch)
	if err != nil {
		return authenticatedHeartRateResponse{}, 0, errors.New("could not encode Heart Rate batch")
	}
	var response authenticatedHeartRateResponse
	status, err := c.doJSON(ctx, http.MethodPost, "/api/heart-rate/batch", token, body, &response)
	if err != nil {
		return authenticatedHeartRateResponse{}, status, err
	}
	if err := validateHeartRateResponse(batch, response); err != nil {
		return response, status, err
	}
	return response, status, nil
}

func validateHeartRateResponse(batch authenticatedHeartRateBatch, response authenticatedHeartRateResponse) error {
	if response.RunID != batch.RunID || response.Received != len(batch.Events) ||
		response.Accepted < 0 || response.Duplicates < 0 || response.Rejected < 0 ||
		response.Received != response.Accepted+response.Duplicates+response.Rejected || len(response.Results) != len(batch.Events) {
		return errors.New("Heart Rate response counters or result count did not match the submitted batch")
	}
	accepted, duplicates, rejected := 0, 0, 0
	for i, result := range response.Results {
		if result.Index != i || result.EventID != batch.Events[i].EventID {
			return fmt.Errorf("Heart Rate response item %d did not match the submitted item order and identity", i)
		}
		switch result.Status {
		case "ACCEPTED":
			accepted++
		case "DUPLICATE":
			duplicates++
		case "REJECTED":
			rejected++
		default:
			return fmt.Errorf("Heart Rate response item %d had an unknown status", i)
		}
	}
	if accepted != response.Accepted || duplicates != response.Duplicates || rejected != response.Rejected {
		return errors.New("Heart Rate response item statuses did not match the response counters")
	}
	return nil
}

func (c *authenticatedHeartRateClient) doJSON(ctx context.Context, method, endpoint, token string, body []byte, result any) (int, error) {
	maxAttempts := c.maxRetries + 1
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		var requestBody io.Reader
		if body != nil {
			requestBody = bytes.NewReader(body)
		}
		request, err := http.NewRequestWithContext(ctx, method, c.baseURL+endpoint, requestBody)
		if err != nil {
			return 0, errors.New("could not prepare authenticated Heart Rate request")
		}
		request.Header.Set("Accept", "application/json")
		if body != nil {
			request.Header.Set("Content-Type", "application/json")
		}
		request.Header.Set("Authorization", "Bearer "+token)
		response, requestErr := c.httpClient.Do(request)
		if requestErr != nil {
			if ctx.Err() != nil {
				return 0, ctx.Err()
			}
			lastErr = errors.New("authenticated Heart Rate request failed")
			if attempt == maxAttempts {
				return 0, lastErr
			}
			if err := waitContext(ctx, c.retryDelay); err != nil {
				return 0, err
			}
			continue
		}
		if response.StatusCode == http.StatusTooManyRequests {
			count := c.rateLimits.Add(1)
			c.logger.Printf("rate_limited endpoint=%s count=%d", endpoint, count)
		}
		if response.StatusCode != http.StatusOK {
			_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maxResponseBodyBytes))
			_ = response.Body.Close()
			lastErr = fmt.Errorf("%s %s returned HTTP %d", method, endpoint, response.StatusCode)
			retryable := response.StatusCode == http.StatusRequestTimeout || response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= http.StatusInternalServerError
			if !retryable || attempt == maxAttempts {
				return response.StatusCode, lastErr
			}
			delay := authRetryDelay(response.Header.Get("Retry-After"), c.retryDelay)
			if delay > maxRetryWait {
				return response.StatusCode, errors.New("Heart Rate retry requested a wait above the 30s cap")
			}
			if err := waitContext(ctx, delay); err != nil {
				return response.StatusCode, err
			}
			continue
		}
		responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, maxResponseBodyBytes+1))
		_ = response.Body.Close()
		if readErr != nil {
			lastErr = errors.New("could not read authenticated Heart Rate response")
			if attempt == maxAttempts {
				return response.StatusCode, lastErr
			}
			if err := waitContext(ctx, c.retryDelay); err != nil {
				return response.StatusCode, err
			}
			continue
		}
		if len(responseBody) > maxResponseBodyBytes {
			return response.StatusCode, errors.New("authenticated Heart Rate response exceeded the size limit")
		}
		if result != nil {
			if err := json.Unmarshal(responseBody, result); err != nil {
				return response.StatusCode, errors.New("authenticated Heart Rate response was invalid")
			}
		}
		return response.StatusCode, nil
	}
	return 0, lastErr
}
