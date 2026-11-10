package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultAccountFrom = 1
	defaultAccountTo   = 1000
	maxAccountIndex    = 1000
	provisionUsage     = "Usage: producer provision [--from INDEX] [--to INDEX]\nCreates missing deterministic test accounts and verifies existing accounts by login."
)

func validateAccountRange(from, to int) error {
	if from < 1 || from > maxAccountIndex {
		return fmt.Errorf("--from must be between 1 and %d", maxAccountIndex)
	}
	if to < 1 || to > maxAccountIndex {
		return fmt.Errorf("--to must be between 1 and %d", maxAccountIndex)
	}
	if from > to {
		return errors.New("--from must be less than or equal to --to")
	}
	return nil
}

func accountFailure(account Account, err error) error {
	return fmt.Errorf("account %d (%s): %w", account.Index, account.Email, err)
}

func parseProvisionArgs(args []string) (int, int, error) {
	fs := flag.NewFlagSet("provision", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.Usage = func() { fmt.Fprintln(os.Stdout, provisionUsage) }
	from := fs.Int("from", defaultAccountFrom, "first deterministic account index")
	to := fs.Int("to", defaultAccountTo, "last deterministic account index")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0, 0, flag.ErrHelp
		}
		return 0, 0, errors.New("invalid provision arguments")
	}
	if fs.NArg() != 0 {
		return 0, 0, errors.New("provision does not accept positional arguments")
	}
	if err := validateAccountRange(*from, *to); err != nil {
		return 0, 0, err
	}
	return *from, *to, nil
}

func authConfigFromEnv(getenv func(string) string) (Config, string, error) {
	if getenv == nil {
		return Config{}, "", errors.New("environment reader is unavailable")
	}
	baseURL := strings.TrimSpace(getenv("VITALOOM_API_BASE_URL"))
	if baseURL == "" {
		return Config{}, "", errors.New("VITALOOM_API_BASE_URL must be set to an authorized test Nginx URL")
	}
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed == nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return Config{}, "", errors.New("VITALOOM_API_BASE_URL must be an absolute http:// or https:// Nginx URL without credentials, query, or fragment")
	}
	password := getenv("VITALOOM_TEST_PASSWORD")
	if len([]rune(password)) < 8 {
		return Config{}, "", errors.New("VITALOOM_TEST_PASSWORD must contain at least eight characters")
	}
	timeout := 5 * time.Second
	if value := strings.TrimSpace(getenv("VITALOOM_TIMEOUT")); value != "" {
		timeout, err = time.ParseDuration(value)
		if err != nil || timeout <= 0 {
			return Config{}, "", errors.New("VITALOOM_TIMEOUT must be a positive duration")
		}
	}
	retries := maxAuthRetries
	if value := strings.TrimSpace(getenv("VITALOOM_MAX_RETRIES")); value != "" {
		retries, err = strconv.Atoi(value)
		if err != nil || retries < 0 || retries > 10 {
			return Config{}, "", errors.New("VITALOOM_MAX_RETRIES must be an integer between 0 and 10")
		}
		if retries > maxAuthRetries {
			retries = maxAuthRetries
		}
	}
	retryDelay := 500 * time.Millisecond
	if value := strings.TrimSpace(getenv("VITALOOM_RETRY_DELAY")); value != "" {
		retryDelay, err = time.ParseDuration(value)
		if err != nil || retryDelay < 0 {
			return Config{}, "", errors.New("VITALOOM_RETRY_DELAY must be a non-negative duration")
		}
	}
	return Config{APIBaseURL: strings.TrimRight(baseURL, "/"), Timeout: timeout, MaxRetries: retries, RetryDelay: retryDelay}, password, nil
}

func runProvision(ctx context.Context, args []string, getenv func(string) string, logger *log.Logger) error {
	from, to, err := parseProvisionArgs(args)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	cfg, password, err := authConfigFromEnv(getenv)
	if err != nil {
		return err
	}
	client := NewAuthClient(cfg, password, logger)
	if err := client.Provision(ctx, from, to); err != nil {
		return err
	}
	if logger != nil {
		logger.Printf("provisioned account range=%04d-%04d count=%d", from, to, to-from+1)
	}
	return nil
}
