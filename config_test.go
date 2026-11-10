package main

import (
	"strings"
	"testing"
	"time"
)

func TestParseRunPlanSelectorsAndDefaults(t *testing.T) {
	env := testRunEnv("http://nginx.test")
	tests := []struct {
		name    string
		args    []string
		metrics MetricSelection
	}{
		{name: "default both", metrics: MetricBoth},
		{name: "Steps selector", args: []string{"step"}, metrics: MetricSteps},
		{name: "Heart Rate selector", args: []string{"heart-rate"}, metrics: MetricHeartRate},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan, _, err := ParseRunPlan(tt.args, env)
			if err != nil {
				t.Fatalf("ParseRunPlan() error = %v", err)
			}
			if plan.Metrics != tt.metrics || plan.AccountIndex != 1 || plan.VirtualUsers != 1 || plan.DevicesPerUser != 1 || plan.BatchSize != 5 || plan.BatchInterval != 5*time.Second || plan.Duration != 0 {
				t.Fatalf("plan = %+v", plan)
			}
			if plan.Config.APIBaseURL != "http://nginx.test" || plan.Config.Timeout != 5*time.Second || plan.Config.MaxRetries != 3 || plan.Config.RetryDelay != 500*time.Millisecond {
				t.Fatalf("config = %+v", plan.Config)
			}
		})
	}
}

func TestParseRunPlanFlagEnvPrecedenceAndIntervalAliases(t *testing.T) {
	envMap := map[string]string{
		"VITALOOM_API_BASE_URL":   "http://nginx.test",
		"VITALOOM_TEST_PASSWORD":  "test-password-123",
		"VITALOOM_BATCH_INTERVAL": "4s",
		"VITALOOM_INTERVAL":       "3s",
		"VITALOOM_BATCH_SIZE":     "7",
		"VITALOOM_DURATION":       "9s",
		"VITALOOM_TIMEOUT":        "2s",
	}
	env := func(key string) string { return envMap[key] }
	plan, _, err := ParseRunPlan([]string{"--interval", "2s", "--batch-size", "4", "--duration", "6s", "--timeout", "8s"}, env)
	if err != nil {
		t.Fatal(err)
	}
	if plan.BatchInterval != 2*time.Second || plan.BatchSize != 4 || plan.Duration != 6*time.Second || plan.Config.Timeout != 8*time.Second {
		t.Fatalf("flags did not override env: %+v", plan)
	}
	plan, _, err = ParseRunPlan([]string{"--interval", "2s", "--batch-interval", "1s"}, env)
	if err != nil || plan.BatchInterval != time.Second {
		t.Fatalf("canonical interval flag should win: interval=%s err=%v", plan.BatchInterval, err)
	}
	plan, _, err = ParseRunPlan(nil, env)
	if err != nil || plan.BatchInterval != 4*time.Second {
		t.Fatalf("canonical interval env should win: interval=%s err=%v", plan.BatchInterval, err)
	}
	delete(envMap, "VITALOOM_BATCH_INTERVAL")
	plan, _, err = ParseRunPlan(nil, env)
	if err != nil || plan.BatchInterval != 3*time.Second {
		t.Fatalf("legacy interval env fallback failed: interval=%s err=%v", plan.BatchInterval, err)
	}
}

func TestParseRunPlanRejectsRemovedMeasurementFlagAndInvalidCombinations(t *testing.T) {
	env := testRunEnv("http://nginx.test")
	for _, args := range [][]string{
		{"--measurement-interval", "1s"},
		{"step", "--measurement-interval", "1s"},
		{"heart-rate", "--measurement-interval", "1s"},
		{"--account", "2", "--virtual-users", "2"},
		{"--virtual-users", "0"},
		{"--virtual-users", "1001"},
		{"--devices-per-user", "2"},
		{"--batch-size", "501"},
	} {
		if _, _, err := ParseRunPlan(args, env); err == nil {
			t.Errorf("ParseRunPlan(%v) unexpectedly succeeded", args)
		}
	}
}

func TestParseRunPlanRequiresConfiguredNginxAndPassword(t *testing.T) {
	for name, env := range map[string]map[string]string{
		"missing URL":      {"VITALOOM_TEST_PASSWORD": "test-password-123"},
		"bad URL":          {"VITALOOM_API_BASE_URL": "http://user:secret@host", "VITALOOM_TEST_PASSWORD": "test-password-123"},
		"missing password": {"VITALOOM_API_BASE_URL": "http://nginx.test"},
	} {
		t.Run(name, func(t *testing.T) {
			lookup := func(key string) string { return env[key] }
			_, _, err := ParseRunPlan(nil, lookup)
			if err == nil || strings.Contains(err.Error(), "secret") {
				t.Fatalf("ParseRunPlan() error = %v", err)
			}
		})
	}
}

func testRunEnv(baseURL string) func(string) string {
	values := map[string]string{"VITALOOM_API_BASE_URL": baseURL, "VITALOOM_TEST_PASSWORD": "test-password-123"}
	return func(key string) string { return values[key] }
}
