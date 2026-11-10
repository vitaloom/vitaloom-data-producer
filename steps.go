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

type authenticatedStepsBatch struct {
	DeviceID string      `json:"deviceId"`
	RunID    string      `json:"runId"`
	Events   []StepEvent `json:"events"`
}

type stepsItemError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type stepsItemResult struct {
	Index   int             `json:"index"`
	EventID string          `json:"eventId"`
	Status  string          `json:"status"`
	Error   *stepsItemError `json:"error,omitempty"`
}

type authenticatedStepsResponse struct {
	RunID      string            `json:"runId"`
	Received   int               `json:"received"`
	Accepted   int               `json:"accepted"`
	Duplicates int               `json:"duplicates"`
	Rejected   int               `json:"rejected"`
	Results    []stepsItemResult `json:"results"`
}

type stepsClient struct {
	baseURL    string
	httpClient *http.Client
	maxRetries int
	retryDelay time.Duration
}

func newStepsClient(cfg Config, _ *log.Logger) *stepsClient {
	return &stepsClient{baseURL: strings.TrimRight(cfg.APIBaseURL, "/"), httpClient: newAuthHTTPClient(cfg.Timeout), maxRetries: cfg.MaxRetries, retryDelay: cfg.RetryDelay}
}

func (c *stepsClient) sendBatch(ctx context.Context, token string, batch authenticatedStepsBatch) (authenticatedStepsResponse, int, error) {
	if len(batch.Events) < 1 || len(batch.Events) > maxBatchSize {
		return authenticatedStepsResponse{}, 0, fmt.Errorf("Steps batch must contain between 1 and %d events", maxBatchSize)
	}
	body, err := json.Marshal(batch)
	if err != nil {
		return authenticatedStepsResponse{}, 0, errors.New("could not encode Steps batch")
	}
	maxAttempts := c.maxRetries + 1
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/steps/batch", bytes.NewReader(body))
		if err != nil {
			return authenticatedStepsResponse{}, 0, errors.New("could not prepare Steps request")
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Accept", "application/json")
		request.Header.Set("Authorization", "Bearer "+token)
		response, requestErr := c.httpClient.Do(request)
		if requestErr != nil {
			lastErr = errors.New("Steps request failed")
			if ctx.Err() != nil || attempt == maxAttempts {
				return authenticatedStepsResponse{}, 0, lastErr
			}
			if err := waitContext(ctx, c.retryDelay); err != nil {
				return authenticatedStepsResponse{}, 0, err
			}
			continue
		}
		status := response.StatusCode
		responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, maxResponseBodyBytes+1))
		_ = response.Body.Close()
		if readErr != nil || len(responseBody) > maxResponseBodyBytes {
			lastErr = errors.New("could not read valid Steps response")
			if attempt == maxAttempts {
				return authenticatedStepsResponse{}, status, lastErr
			}
			if err := waitContext(ctx, c.retryDelay); err != nil {
				return authenticatedStepsResponse{}, status, err
			}
			continue
		}
		if status != http.StatusOK {
			lastErr = fmt.Errorf("POST /api/steps/batch returned HTTP %d", status)
			retryable := status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= http.StatusInternalServerError
			if !retryable || attempt == maxAttempts {
				return authenticatedStepsResponse{}, status, lastErr
			}
			if err := waitContext(ctx, c.retryDelay); err != nil {
				return authenticatedStepsResponse{}, status, err
			}
			continue
		}
		var result authenticatedStepsResponse
		if err := json.Unmarshal(responseBody, &result); err != nil {
			lastErr = fmt.Errorf("decode Steps response: %w", err)
		} else if err := validateStepsResponse(batch, result); err != nil {
			lastErr = err
		} else {
			return result, status, nil
		}
		if attempt == maxAttempts {
			return authenticatedStepsResponse{}, status, lastErr
		}
		if err := waitContext(ctx, c.retryDelay); err != nil {
			return authenticatedStepsResponse{}, status, err
		}
	}
	return authenticatedStepsResponse{}, 0, errors.New("Steps request exhausted retries")
}

func validateStepsResponse(batch authenticatedStepsBatch, response authenticatedStepsResponse) error {
	if response.RunID != batch.RunID || response.Received != len(batch.Events) ||
		response.Accepted < 0 || response.Duplicates < 0 || response.Rejected < 0 ||
		response.Received != response.Accepted+response.Duplicates+response.Rejected || len(response.Results) != len(batch.Events) {
		return errors.New("Steps response counters or result count did not match the submitted batch")
	}
	accepted, duplicates, rejected := 0, 0, 0
	for i, result := range response.Results {
		if result.Index != i || result.EventID != batch.Events[i].EventID {
			return fmt.Errorf("Steps response item %d did not match the submitted item order and identity", i)
		}
		switch result.Status {
		case "ACCEPTED":
			if result.Error != nil {
				return fmt.Errorf("Steps response item %d included an error for ACCEPTED status", i)
			}
			accepted++
		case "DUPLICATE":
			if result.Error != nil {
				return fmt.Errorf("Steps response item %d included an error for DUPLICATE status", i)
			}
			duplicates++
		case "REJECTED":
			if result.Error == nil || strings.TrimSpace(result.Error.Code) == "" {
				return fmt.Errorf("Steps response item %d did not include an error code for REJECTED status", i)
			}
			rejected++
		default:
			return fmt.Errorf("Steps response item %d had an unknown status", i)
		}
	}
	if accepted != response.Accepted || duplicates != response.Duplicates || rejected != response.Rejected {
		return errors.New("Steps response item statuses did not match the response counters")
	}
	return nil
}

func stepsDeviceID(accountIndex int) string {
	return fmt.Sprintf("steps-device-loadtest-%04d", accountIndex)
}
