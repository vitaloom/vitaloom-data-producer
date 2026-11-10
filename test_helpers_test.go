package main

import (
	"net/http"
	"testing"
)

func assertBearer(t *testing.T, r *http.Request, token string) {
	t.Helper()
	if got := r.Header.Get("Authorization"); got != "Bearer "+token {
		t.Errorf("Authorization = %q; want Bearer token", got)
	}
}
