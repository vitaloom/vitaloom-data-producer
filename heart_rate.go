package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	heartRateMeasurementInterval = time.Second
	heartRateBatchInterval       = 5 * time.Second
	heartRateQueueLimit          = 1000
	heartRateShutdownBudget      = 5 * time.Second
	heartRateRefreshBefore       = 30 * time.Second
)

const heartRateSampleCount = 3

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

type heartRateSummarySample struct {
	HeartRateBPM int       `json:"heartRateBpm"`
	OccurredAt   time.Time `json:"occurredAt"`
}

type heartRateDailyAverage struct {
	HeartRateBPM *float64 `json:"heartRateBpm"`
	DataStatus   string   `json:"dataStatus"`
}

type heartRateSummary struct {
	Date           string                  `json:"date"`
	Timezone       string                  `json:"timezone"`
	AsOf           time.Time               `json:"asOf"`
	LatestRecorded *heartRateSummarySample `json:"latestRecorded"`
	DailyAverage   heartRateDailyAverage   `json:"dailyAverage"`
}

type authenticatedHeartRateClient struct {
	baseURL    string
	httpClient *http.Client
	maxRetries int
	retryDelay time.Duration
	logger     *log.Logger
	rateLimits int
}

func newAuthenticatedHeartRateClient(cfg Config, logger *log.Logger) *authenticatedHeartRateClient {
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	return &authenticatedHeartRateClient{
		baseURL:    strings.TrimRight(cfg.APIBaseURL, "/"),
		httpClient: newAuthHTTPClient(cfg.Timeout),
		maxRetries: cfg.MaxRetries,
		retryDelay: cfg.RetryDelay,
		logger:     logger,
	}
}

func (c *authenticatedHeartRateClient) sendBatch(ctx context.Context, token string, batch authenticatedHeartRateBatch) (authenticatedHeartRateResponse, int, error) {
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
		response.Received != response.Accepted+response.Duplicates+response.Rejected ||
		len(response.Results) != len(batch.Events) {
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

func (c *authenticatedHeartRateClient) readSummary(ctx context.Context, token string) (heartRateSummary, int, error) {
	var summary heartRateSummary
	status, err := c.doJSON(ctx, http.MethodGet, "/api/heart-rate/summary", token, nil, &summary)
	if err != nil {
		return heartRateSummary{}, status, err
	}
	return summary, status, nil
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
			c.rateLimits++
			c.logger.Printf("rate_limited endpoint=%s count=%d", endpoint, c.rateLimits)
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

func waitContext(ctx context.Context, delay time.Duration) error {
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

func parseHeartRateCommand(args []string, getenv func(string) string) (Config, string, int, time.Duration, time.Duration, time.Duration, bool, error) {
	fs := flag.NewFlagSet("heart-rate", flag.ContinueOnError)
	fs.SetOutput(os.Stdout)
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "Usage: producer heart-rate --account INDEX [--measurement-interval DURATION --batch-interval DURATION [--duration DURATION]]")
		fmt.Fprintln(fs.Output(), "Without scheduling flags, submits one Heart Rate batch and reads the account summary.")
	}
	accountIndex := fs.Int("account", 0, "deterministic test account index (1-1000)")
	measurementText := fs.String("measurement-interval", heartRateMeasurementInterval.String(), "time between Heart Rate measurements")
	batchText := fs.String("batch-interval", heartRateBatchInterval.String(), "time between Heart Rate batch flushes")
	durationText := fs.String("duration", "0s", "scheduled run duration; 0s means run until Ctrl+C")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return Config{}, "", 0, 0, 0, 0, false, flag.ErrHelp
		}
		return Config{}, "", 0, 0, 0, 0, false, errors.New("invalid heart-rate command arguments")
	}
	if fs.NArg() != 0 {
		return Config{}, "", 0, 0, 0, 0, false, errors.New("heart-rate does not accept positional arguments")
	}
	scheduled := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "measurement-interval" || f.Name == "batch-interval" || f.Name == "duration" {
			scheduled = true
		}
	})
	if err := validateAccountRange(*accountIndex, *accountIndex); err != nil {
		return Config{}, "", 0, 0, 0, 0, false, errors.New("--account must be between 1 and 1000")
	}
	measurementInterval, err := time.ParseDuration(*measurementText)
	if err != nil || measurementInterval <= 0 {
		return Config{}, "", 0, 0, 0, 0, false, errors.New("--measurement-interval must be a positive duration")
	}
	batchInterval, err := time.ParseDuration(*batchText)
	if err != nil || batchInterval <= 0 {
		return Config{}, "", 0, 0, 0, 0, false, errors.New("--batch-interval must be a positive duration")
	}
	duration, err := time.ParseDuration(*durationText)
	if err != nil || duration < 0 {
		return Config{}, "", 0, 0, 0, 0, false, errors.New("--duration must be zero or a positive duration")
	}
	cfg, password, err := authConfigFromEnv(getenv)
	if err != nil {
		return Config{}, "", 0, 0, 0, 0, false, err
	}
	return cfg, password, *accountIndex, measurementInterval, batchInterval, duration, scheduled, nil
}

func runHeartRateOnce(ctx context.Context, cfg Config, password string, accountIndex int, logger *log.Logger) error {
	account := deterministicAccount(accountIndex)
	auth := NewAuthClient(cfg, password, logger)
	session, err := auth.Login(ctx, account.Email)
	if err != nil {
		return accountFailure(account, err)
	}
	runID, err := NewRunID(time.Now())
	if err != nil {
		return err
	}
	generatedAt := time.Now().UTC().Truncate(time.Millisecond)
	generator := NewEventGenerator(runID, generatedAt, nil)
	batch := authenticatedHeartRateBatch{
		DeviceID: fmt.Sprintf("device-loadtest-%04d", accountIndex),
		RunID:    runID,
		Events:   make([]HeartRateEvent, 0, heartRateSampleCount),
	}
	for i := 0; i < heartRateSampleCount; i++ {
		occurredAt := generatedAt.Add(-time.Duration(heartRateSampleCount-1-i) * time.Second)
		phase := generator.activityPhase(occurredAt)
		batch.Events = append(batch.Events, generator.nextHeartRateEvent(occurredAt, phase))
	}

	client := newAuthenticatedHeartRateClient(cfg, logger)
	response, status, err := client.sendBatch(ctx, session.AccessToken, batch)
	if err != nil {
		return fmt.Errorf("Heart Rate batch failed (HTTP %d, rate_limited=%d): %w", status, client.rateLimits, err)
	}
	logger.Printf("heart_rate_batch run_id=%s device_id=%s received=%d accepted=%d duplicates=%d rejected=%d rate_limited=%d", runID, batch.DeviceID, response.Received, response.Accepted, response.Duplicates, response.Rejected, client.rateLimits)
	for _, item := range response.Results {
		code := ""
		if item.Error != nil {
			code = item.Error.Code
		}
		event := batch.Events[item.Index]
		logger.Printf("heart_rate_result index=%d event_id=%s status=%s error_code=%s bpm=%d occurred_at=%s", item.Index, item.EventID, item.Status, code, event.HeartRateBPM, event.OccurredAt.Format(time.RFC3339Nano))
	}
	summary, summaryStatus, err := client.readSummary(ctx, session.AccessToken)
	if err != nil {
		return fmt.Errorf("Heart Rate batch outcomes reported; summary GET failed (HTTP %d, rate_limited=%d): %w", summaryStatus, client.rateLimits, err)
	}
	logHeartRateSummary(logger, summary)
	if response.Rejected > 0 {
		return fmt.Errorf("Heart Rate batch completed with %d rejected item(s)", response.Rejected)
	}
	return nil
}

func logHeartRateSummary(logger *log.Logger, summary heartRateSummary) {
	if summary.LatestRecorded == nil {
		logger.Printf("heart_rate_summary latest=none daily_average=%s daily_average_status=%s date=%s", nullableAverage(summary.DailyAverage.HeartRateBPM), summary.DailyAverage.DataStatus, summary.Date)
		return
	}
	logger.Printf("heart_rate_summary latest_bpm=%d latest_occurred_at=%s daily_average=%s daily_average_status=%s date=%s", summary.LatestRecorded.HeartRateBPM, summary.LatestRecorded.OccurredAt.UTC().Format(time.RFC3339Nano), nullableAverage(summary.DailyAverage.HeartRateBPM), summary.DailyAverage.DataStatus, summary.Date)
}

func nullableAverage(value *float64) string {
	if value == nil {
		return "none"
	}
	return fmt.Sprintf("%.2f", *value)
}
