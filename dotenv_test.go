package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadDotEnvPreservesLiteralValuesAndEnvironmentPrecedence(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env")
	content := "\uFEFF# local test settings\r\n\r\nVITALOOM_API_BASE_URL=http://localhost\r\nVITALOOM_TEST_PASSWORD=pa#ss= word \r\nVITALOOM_TIMEOUT=9s\r\n"
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	values, err := LoadDotEnv(path)
	if err != nil {
		t.Fatalf("LoadDotEnv() error = %v", err)
	}
	if got := values["VITALOOM_TEST_PASSWORD"]; got != "pa#ss= word " {
		t.Fatalf("password value = %q; want literal spaces and #/= preserved", got)
	}
	t.Setenv("VITALOOM_API_BASE_URL", "")
	lookup := environmentLookup(values)
	if got := lookup("VITALOOM_API_BASE_URL"); got != "" {
		t.Fatalf("explicit empty process value = %q; want empty", got)
	}
	if got := lookup("VITALOOM_TEST_PASSWORD"); got != "pa#ss= word " {
		t.Fatalf("dotenv password = %q", got)
	}
}

func TestLoadDotEnvMissingFileAndReportsMalformedLine(t *testing.T) {
	values, err := LoadDotEnv(filepath.Join(t.TempDir(), "missing"))
	if err != nil || len(values) != 0 {
		t.Fatalf("missing dotenv = %#v, %v; want empty settings and no error", values, err)
	}
	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte("VITALOOM_API_BASE_URL=http://localhost\nnot-an-assignment\n"), 0600); err != nil {
		t.Fatal(err)
	}
	_, err = LoadDotEnv(path)
	if err == nil || !strings.Contains(err.Error(), "line 2") {
		t.Fatalf("malformed dotenv error = %v; want line 2", err)
	}
}
