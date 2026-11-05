package main

import (
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const maxBatchSize = 500

type Config struct {
	APIBaseURL string
	BatchSize int
	Interval time.Duration
	Duration time.Duration
	Timeout time.Duration
	MaxRetries int
	RetryDelay time.Duration
}

func ParseConfig(args []string, getenv func(string) string) (Config, error) {
	if getenv == nil { getenv = os.Getenv }
	envOr := func(key, fallback string) string {
		value := strings.TrimSpace(getenv(key))
		if value == "" { return fallback }
		return value
	}
	fs := flag.NewFlagSet("vitaloom-producer", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "VitaLoom Go Health Metrics Producer")
		fmt.Fprintln(fs.Output(), "Runs continuously by default. Press Ctrl+C to stop; set --duration for a bounded run.")
		fmt.Fprintln(fs.Output(), "")
		fmt.Fprintln(fs.Output(), "Flags:")
		fs.PrintDefaults()
	}
	apiBaseURL := fs.String("api-base-url", envOr("VITALOOM_API_BASE_URL", "http://localhost"), "API/Nginx base URL")
	batchSizeText := fs.String("batch-size", envOr("VITALOOM_BATCH_SIZE", "5"), "events in each batch per metric (1-500)")
	intervalText := fs.String("interval", envOr("VITALOOM_INTERVAL", "5s"), "time between batch cycles, e.g. 2s or 500ms")
	durationText := fs.String("duration", envOr("VITALOOM_DURATION", "0s"), "optional run duration; 0s means run indefinitely")
	timeoutText := fs.String("timeout", envOr("VITALOOM_TIMEOUT", "5s"), "timeout per HTTP attempt")
	maxRetriesText := fs.String("max-retries", envOr("VITALOOM_MAX_RETRIES", "3"), "maximum retries after the initial request (0-10)")
	retryDelayText := fs.String("retry-delay", envOr("VITALOOM_RETRY_DELAY", "500ms"), "delay between retries")
	if err := fs.Parse(args); err != nil { return Config{}, err }
	if fs.NArg() > 0 { return Config{}, fmt.Errorf("unexpected positional arguments: %s", strings.Join(fs.Args(), " ")) }

	batchSize, err := strconv.Atoi(*batchSizeText)
	if err != nil { return Config{}, fmt.Errorf("invalid --batch-size %q: must be an integer", *batchSizeText) }
	interval, err := time.ParseDuration(*intervalText)
	if err != nil { return Config{}, fmt.Errorf("invalid --interval %q: %w", *intervalText, err) }
	duration, err := time.ParseDuration(*durationText)
	if err != nil { return Config{}, fmt.Errorf("invalid --duration %q: %w", *durationText, err) }
	timeout, err := time.ParseDuration(*timeoutText)
	if err != nil { return Config{}, fmt.Errorf("invalid --timeout %q: %w", *timeoutText, err) }
	maxRetries, err := strconv.Atoi(*maxRetriesText)
	if err != nil { return Config{}, fmt.Errorf("invalid --max-retries %q: must be an integer", *maxRetriesText) }
	retryDelay, err := time.ParseDuration(*retryDelayText)
	if err != nil { return Config{}, fmt.Errorf("invalid --retry-delay %q: %w", *retryDelayText, err) }
	parsedURL, err := url.Parse(strings.TrimSpace(*apiBaseURL))
	if err != nil || parsedURL == nil || (parsedURL.Scheme != "http" && parsedURL.Scheme != "https") || parsedURL.Host == "" {
		return Config{}, fmt.Errorf("invalid --api-base-url %q: expected an absolute http:// or https:// URL", *apiBaseURL)
	}
	if parsedURL.User != nil || parsedURL.RawQuery != "" || parsedURL.Fragment != "" {
		return Config{}, errors.New("--api-base-url must not contain credentials, a query string, or a fragment")
	}
	if batchSize < 1 || batchSize > maxBatchSize { return Config{}, fmt.Errorf("--batch-size must be between 1 and %d", maxBatchSize) }
	if interval <= 0 { return Config{}, errors.New("--interval must be greater than zero") }
	if duration < 0 { return Config{}, errors.New("--duration must be zero (run indefinitely) or greater than zero") }
	if timeout <= 0 { return Config{}, errors.New("--timeout must be greater than zero") }
	if maxRetries < 0 || maxRetries > 10 { return Config{}, errors.New("--max-retries must be between 0 and 10") }
	if retryDelay < 0 { return Config{}, errors.New("--retry-delay must not be negative") }
	return Config{APIBaseURL: strings.TrimRight(strings.TrimSpace(*apiBaseURL), "/"), BatchSize: batchSize, Interval: interval, Duration: duration, Timeout: timeout, MaxRetries: maxRetries, RetryDelay: retryDelay}, nil
}
