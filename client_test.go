package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func testClient(serverURL string,retries int,delay time.Duration)*APIClient {
	return &APIClient{baseURL:serverURL,httpClient:&http.Client{Timeout:time.Second},maxRetries:retries,retryDelay:delay,logger:log.New(io.Discard,"",0)}
}
func TestClientRetriesTransientFailureWithIdenticalBody(t *testing.T) {
	var requests atomic.Int32
	var firstBody string
	server:=httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,r *http.Request) {
		if r.URL.Path!="/api/heart-rate/batch" {t.Errorf("unexpected path: %s",r.URL.Path)}
		if r.Method!=http.MethodPost {t.Errorf("method = %s",r.Method)}
		if got:=r.Header.Get("Content-Type");got!="application/json" {t.Errorf("Content-Type = %q",got)}
		body,err:=io.ReadAll(r.Body);if err!=nil {t.Errorf("read body: %v",err)}
		if requests.Add(1)==1 {firstBody=string(body);http.Error(w,"temporary outage",http.StatusServiceUnavailable);return}
		if string(body)!=firstBody {t.Errorf("retry body changed: first=%s retry=%s",firstBody,string(body))}
		w.Header().Set("Content-Type","application/json");w.WriteHeader(http.StatusAccepted)
		_,_=fmt.Fprintf(w,"{\"runId\":\"run-x\",\"received\":1,\"accepted\":1,\"duplicates\":0}")
	}))
	defer server.Close()
	client:=testClient(server.URL,2,0)
	batch:=HeartRateBatch{RunID:"run-x",Events:[]HeartRateEvent{{EventID:"hr-run-x-000001",OccurredAt:time.Date(2026,10,24,2,0,0,0,time.UTC),HeartRateBPM:92}}}
	result,err:=client.SendHeartRateBatch(context.Background(),batch)
	if err!=nil {t.Fatalf("SendHeartRateBatch() error = %v",err)}
	if requests.Load()!=2 {t.Fatalf("requests = %d; want 2",requests.Load())}
	if result.Attempts!=2||result.StatusCode!=http.StatusAccepted {t.Errorf("result = %+v",result)}
	if result.Response.Accepted!=1||result.Response.Duplicates!=0 {t.Errorf("response = %+v",result.Response)}
}
func TestClientDoesNotRetryNonTransient4xx(t *testing.T) {
	var requests atomic.Int32
	server:=httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,r *http.Request) {
		requests.Add(1);http.Error(w,"{\"error\":{\"code\":\"VALIDATION_ERROR\"}}",http.StatusBadRequest)
	}))
	defer server.Close()
	client:=testClient(server.URL,3,0)
	batch:=StepBatch{RunID:"run-x",Events:[]StepEvent{{EventID:"steps-run-x-000001",OccurredAt:time.Now().UTC(),StepsDelta:4}}}
	result,err:=client.SendStepBatch(context.Background(),batch)
	if err==nil||!strings.Contains(err.Error(),"HTTP 400") {t.Fatalf("error = %v",err)}
	if requests.Load()!=1||result.Attempts!=1 {t.Fatalf("requests=%d result=%+v; expected one attempt",requests.Load(),result)}
}
func TestClientRejectsMismatchedResponse(t *testing.T) {
	server:=httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,r *http.Request) {
		w.Header().Set("Content-Type","application/json");w.WriteHeader(http.StatusAccepted)
		_=json.NewEncoder(w).Encode(BatchIngestionResponse{RunID:"other-run",Received:1,Accepted:1,Duplicates:0})
	}))
	defer server.Close()
	client:=testClient(server.URL,0,0)
	batch:=StepBatch{RunID:"run-x",Events:[]StepEvent{{EventID:"steps-run-x-000001",OccurredAt:time.Now().UTC(),StepsDelta:4}}}
	_,err:=client.SendStepBatch(context.Background(),batch)
	if err==nil||!strings.Contains(err.Error(),"unexpected steps response") {t.Fatalf("error = %v",err)}
}
