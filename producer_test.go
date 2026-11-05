package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	mathrand "math/rand"
)
func TestRunProducerWithoutDurationRunsUntilStopped(t *testing.T) {
	var requestCount atomic.Int32
	ctx,cancel:=context.WithCancel(context.Background());defer cancel()
	server:=httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,r *http.Request) {
		var body struct {RunID string `json:"runId"`;Events []json.RawMessage `json:"events"`}
		if err:=json.NewDecoder(r.Body).Decode(&body);err!=nil {t.Errorf("decode body: %v",err)}
		count:=requestCount.Add(1)
		w.Header().Set("Content-Type","application/json");w.WriteHeader(http.StatusAccepted)
		_=json.NewEncoder(w).Encode(BatchIngestionResponse{RunID:body.RunID,Received:len(body.Events),Accepted:len(body.Events),Duplicates:0})
		if count==4 {cancel()}
	}))
	defer server.Close()
	cfg:=Config{APIBaseURL:server.URL,BatchSize:2,Interval:time.Millisecond,Duration:0,Timeout:time.Second,MaxRetries:0}
	logger:=log.New(io.Discard,"",0)
	stats,err:=runProducer(ctx,cfg,NewAPIClient(cfg,logger),logger,NewEventGenerator("run-loop",time.Now(),mathrand.New(mathrand.NewSource(4))))
	if err!=nil {t.Fatalf("runProducer() error = %v",err)}
	if requestCount.Load()!=4 {t.Fatalf("requests = %d; want 4 (two full cycles)",requestCount.Load())}
	if stats.Cycles!=2||stats.SuccessfulBatches!=4||stats.EventsGenerated!=8||stats.EventsAccepted!=8 {t.Fatalf("unexpected summary: %+v",stats)}
}
func TestRunProducerDurationStopsAfterCurrentCycle(t *testing.T) {
	var requestCount atomic.Int32
	server:=httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,r *http.Request) {
		var body struct {RunID string `json:"runId"`;Events []json.RawMessage `json:"events"`}
		if err:=json.NewDecoder(r.Body).Decode(&body);err!=nil {t.Errorf("decode body: %v",err)}
		requestCount.Add(1);w.Header().Set("Content-Type","application/json");w.WriteHeader(http.StatusAccepted)
		_=json.NewEncoder(w).Encode(BatchIngestionResponse{RunID:body.RunID,Received:len(body.Events),Accepted:len(body.Events),Duplicates:0})
	}))
	defer server.Close()
	cfg:=Config{APIBaseURL:server.URL,BatchSize:1,Interval:5*time.Millisecond,Duration:25*time.Millisecond,Timeout:time.Second,MaxRetries:0}
	logger:=log.New(io.Discard,"",0)
	stats,err:=runProducer(context.Background(),cfg,NewAPIClient(cfg,logger),logger,NewEventGenerator("run-duration",time.Now(),mathrand.New(mathrand.NewSource(5))))
	if err!=nil {t.Fatalf("runProducer() error = %v",err)}
	if stats.Cycles<1 {t.Fatalf("expected at least one cycle: %+v",stats)}
	if requestCount.Load()!=int32(stats.Cycles*2) {t.Fatalf("request count=%d, cycles=%d",requestCount.Load(),stats.Cycles)}
	if stats.Duration()<cfg.Duration {t.Errorf("duration %s ended before requested %s",stats.Duration(),cfg.Duration)}
}
func TestRunProducerReportsFailedBatch(t *testing.T) {
	var requestCount atomic.Int32
	server:=httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,r *http.Request) {
		requestCount.Add(1);http.Error(w,"{\"error\":{\"code\":\"VALIDATION_ERROR\"}}",http.StatusBadRequest)
	}))
	defer server.Close()
	cfg:=Config{APIBaseURL:server.URL,BatchSize:1,Interval:time.Millisecond,Duration:0,Timeout:time.Second,MaxRetries:3}
	logger:=log.New(io.Discard,"",0)
	stats,err:=runProducer(context.Background(),cfg,NewAPIClient(cfg,logger),logger,NewEventGenerator("run-failed",time.Now(),mathrand.New(mathrand.NewSource(6))))
	if err==nil {t.Fatal("expected failed batch error")}
	if requestCount.Load()!=1 {t.Fatalf("requests=%d; non-transient 400 must not retry",requestCount.Load())}
	if stats.FailedBatches!=1||stats.SuccessfulBatches!=0 {t.Fatalf("unexpected failure summary: %+v",stats)}
	if !strings.Contains(err.Error(),"HTTP 400") {t.Fatalf("error missing HTTP status: %v",err)}
}
