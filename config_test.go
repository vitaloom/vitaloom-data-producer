package main

import (
	"strings"
	"testing"
	"time"
)

func TestParseConfigDefaultsToUnlimitedRun(t *testing.T) {
	cfg, err := ParseConfig(nil, func(string) string { return "" })
	if err != nil { t.Fatalf("ParseConfig() error = %v", err) }
	if cfg.APIBaseURL != "http://localhost" { t.Errorf("APIBaseURL = %q", cfg.APIBaseURL) }
	if cfg.BatchSize != 5 { t.Errorf("BatchSize = %d", cfg.BatchSize) }
	if cfg.Interval != 5*time.Second { t.Errorf("Interval = %s", cfg.Interval) }
	if cfg.Duration != 0 { t.Errorf("Duration = %s; want unlimited (0)", cfg.Duration) }
	if cfg.MaxRetries != 3 { t.Errorf("MaxRetries = %d", cfg.MaxRetries) }
}

func TestParseConfigFlagsOverrideEnvironment(t *testing.T) {
	env := map[string]string{"VITALOOM_API_BASE_URL":"http://env.example/","VITALOOM_BATCH_SIZE":"8","VITALOOM_INTERVAL":"3s","VITALOOM_DURATION":"20s"}
	cfg, err := ParseConfig([]string{"--api-base-url","http://localhost:8080","--batch-size","2","--duration","0s"}, func(key string) string { return env[key] })
	if err != nil { t.Fatalf("ParseConfig() error = %v", err) }
	if cfg.APIBaseURL != "http://localhost:8080" || cfg.BatchSize != 2 || cfg.Duration != 0 { t.Fatalf("flags did not override env defaults: %+v", cfg) }
	if cfg.Interval != 3*time.Second { t.Errorf("Interval = %s; want 3s", cfg.Interval) }
}

func TestParseConfigRejectsInvalidValues(t *testing.T) {
	tests := []struct{name string; args []string; want string}{
		{"invalid URL",[]string{"--api-base-url","localhost"},"absolute http:// or https:// URL"},
		{"zero batch",[]string{"--batch-size","0"},"between 1 and 500"},
		{"too large batch",[]string{"--batch-size","501"},"between 1 and 500"},
		{"negative interval",[]string{"--interval","-1s"},"greater than zero"},
		{"negative duration",[]string{"--duration","-1s"},"zero (run indefinitely)"},
		{"too many retries",[]string{"--max-retries","11"},"between 0 and 10"},
		{"negative retry delay",[]string{"--retry-delay","-1ms"},"must not be negative"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseConfig(tt.args, func(string) string { return "" })
			if err == nil || !strings.Contains(err.Error(), tt.want) { t.Fatalf("error = %v; want substring %q", err, tt.want) }
		})
	}
}
