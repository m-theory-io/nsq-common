//go:build ignore

package nsqcommon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/nsqio/go-nsq"
)

type fakeProducer struct {
	mu      sync.Mutex
	err     error
	bodies  [][]byte
	stopped bool
}

func (p *fakeProducer) Publish(_ string, body []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.bodies = append(p.bodies, append([]byte(nil), body...))
	return p.err
}

func (p *fakeProducer) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stopped = true
}

func testClient(t *testing.T) *Client {
	t.Helper()
	c := NewClient(Config{CreationRetryDelay: time.Nanosecond, PublishRetryDelay: time.Nanosecond})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := c.Close(ctx); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return c
}

func lookupServer(t *testing.T, producers string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/lookup" || r.URL.Query().Get("topic") != "topic" {
			t.Errorf("unexpected lookup request: %s", r.URL)
		}
		fmt.Fprintf(w, `{"producers":%s}`, producers)
	}))
	t.Cleanup(server.Close)
	return server
}

func TestPublishWireFormatsAndMetrics(t *testing.T) {
	c := testClient(t)
	p := &fakeProducer{}
	created := 0
	c.newProducer = func(addr string) (producer, error) {
		if addr != "fallback:4150" {
			t.Errorf("unexpected address %s", addr)
		}
		created++
		return p, nil
	}
	envelope := struct {
		Type          string `json:"message_type"`
		CorrelationID string `json:"correlation_id,omitempty"`
	}{Type: "login"}
	for _, value := range []interface{}{
		envelope,
		map[string]interface{}{"block": "original", "count": 2},
		json.RawMessage(`{"raw":true}`),
	} {
		if err := c.PublishJSON("", "fallback:4150", "topic", value); err != nil {
			t.Fatal(err)
		}
	}
	binary := []byte{0, 1, 255}
	if err := c.Publish("", "fallback:4150", "topic", binary); err != nil {
		t.Fatal(err)
	}
	want := [][]byte{
		[]byte(`{"message_type":"login"}`),
		[]byte(`{"block":"original","count":2}`),
		[]byte(`{"raw":true}`),
		binary,
	}
	if !reflect.DeepEqual(p.bodies, want) || created != 1 {
		t.Fatalf("bodies=%q created=%d", p.bodies, created)
	}
	metrics := c.UsageMetrics()
	if metrics["fallback:4150"] != 4 {
		t.Fatalf("metrics=%v", metrics)
	}
	metrics["fallback:4150"] = 99
	if c.UsageMetrics()["fallback:4150"] != 4 {
		t.Fatal("metrics exposed mutable client state")
	}
	c.ResetUsageMetrics()
	if len(c.UsageMetrics()) != 0 {
		t.Fatal("metrics not reset")
	}
}

func TestDiscoveryRoundRobinAndDNS(t *testing.T) {
	server := lookupServer(t, `[{"broadcast_address":"bad","tcp_port":4150},{"broadcast_address":"good","tcp_port":4150}]`)
	c := testClient(t)
	for i, want := range []string{"bad:4150", "good:4150", "bad:4150"} {
		got, err := c.selectAddress(server.URL, "", "topic")
		if err != nil || got != want {
			t.Fatalf("selection %d: got=%q err=%v want=%q", i, got, err, want)
		}
	}
	c.SetProducerDNSVerification(true)
	c.resolveHost = func(_ context.Context, host string) bool { return host == "good" }
	if got, err := c.selectAddress(server.URL, "", "topic"); err != nil || got != "good:4150" {
		t.Fatalf("filtered selection=%q err=%v", got, err)
	}
	c.resolveHost = func(context.Context, string) bool { return false }
	if got, err := c.selectAddress(server.URL, "fallback:4150", "topic"); err != nil || got != "fallback:4150" {
		t.Fatalf("DNS fallback=%q err=%v", got, err)
	}
	got, err := c.selectAddress(server.URL, "", "topic")
	if err != nil || (got != "good:4150" && got != "bad:4150") {
		t.Fatalf("original-list fallback=%q err=%v", got, err)
	}
}

func TestDiscoveryTriesNextLookupdAndSupportsIPv6(t *testing.T) {
	invalid := lookupServer(t, `[{"broadcast_address":"","tcp_port":4150}]`)
	valid := lookupServer(t, `[{"broadcast_address":"::1","tcp_port":4150}]`)
	c := testClient(t)
	got, err := c.selectAddress(" , "+invalid.URL+", "+valid.URL+"/, ", "", "topic")
	if err != nil || got != "[::1]:4150" {
		t.Fatalf("selection=%q err=%v", got, err)
	}
}

func TestDiscoveryHTTPFailures(t *testing.T) {
	for _, test := range []struct {
		name   string
		status int
		body   string
	}{
		{"unavailable", http.StatusServiceUnavailable, "offline"},
		{"invalid JSON", http.StatusOK, "{"},
		{"empty producers", http.StatusOK, `{"producers":[]}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(test.status)
				fmt.Fprint(w, test.body)
			}))
			defer server.Close()
			c := testClient(t)
			got, err := c.selectAddress(server.URL, "fallback:4150", "topic")
			if err != nil || got != "fallback:4150" {
				t.Fatalf("fallback=%q err=%v", got, err)
			}
			if _, err := c.selectAddress(server.URL, "", "topic"); err == nil {
				t.Fatal("expected discovery failure without fallback")
			}
		})
	}
}

func TestPublishRejectsInvalidInput(t *testing.T) {
	c := testClient(t)
	c.newProducer = func(string) (producer, error) {
		t.Fatal("must not create producer for invalid input")
		return nil, nil
	}
	for _, err := range []error{
		c.Publish("", "", "topic", nil),
		c.Publish("", "fallback:4150", "bad topic", nil),
		c.PublishJSON("", "fallback:4150", "topic", make(chan int)),
	} {
		if err == nil {
			t.Fatal("expected input error")
		}
	}
}

func TestPublishRetriesAndRetiresProducer(t *testing.T) {
	c := testClient(t)
	failed := &fakeProducer{err: nsq.ErrNotConnected}
	succeeded := &fakeProducer{}
	calls := 0
	c.newProducer = func(string) (producer, error) {
		calls++
		if calls == 1 {
			return failed, nil
		}
		return succeeded, nil
	}
	if err := c.Publish("", "fallback:4150", "topic", []byte("body")); err != nil {
		t.Fatal(err)
	}
	if calls != 2 || !failed.stopped || len(succeeded.bodies) != 1 {
		t.Fatalf("calls=%d stopped=%v successful publishes=%d", calls, failed.stopped, len(succeeded.bodies))
	}
	if c.UsageMetrics()["fallback:4150"] != 1 {
		t.Fatal("failed attempt counted as success")
	}
}

func TestPublishProtocolErrorsReuseProducer(t *testing.T) {
	c := testClient(t)
	p := &fakeProducer{err: nsq.ErrProtocol{Reason: "E_PUB_FAILED"}}
	c.newProducer = func(string) (producer, error) { return p, nil }
	err := c.Publish("", "fallback:4150", "topic", nil)
	var protocolErr nsq.ErrProtocol
	if !errors.As(err, &protocolErr) || len(p.bodies) != 3 || p.stopped {
		t.Fatalf("err=%v calls=%d stopped=%v", err, len(p.bodies), p.stopped)
	}
}

func TestPublishExhaustionPreservesCause(t *testing.T) {
	cause := errors.New("producer creation failed")
	c := testClient(t)
	calls := 0
	c.newProducer = func(string) (producer, error) {
		calls++
		return nil, cause
	}
	if err := c.Publish("", "fallback:4150", "topic", nil); !errors.Is(err, cause) || calls != 3 {
		t.Fatalf("err=%v attempts=%d", err, calls)
	}

	p := &fakeProducer{err: &net.OpError{Op: "dial", Net: "tcp", Err: cause}}
	c.newProducer = func(string) (producer, error) { return p, nil }
	err := c.Publish("", "fallback:4150", "topic", nil)
	if !errors.Is(err, cause) || len(p.bodies) != 3 || !p.stopped {
		t.Fatalf("err=%v attempts=%d stopped=%v", err, len(p.bodies), p.stopped)
	}
}

func TestWaitForLookupd(t *testing.T) {
	var mu sync.Mutex
	healthy := true
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ping" {
			t.Errorf("unexpected readiness URL %s", r.URL)
		}
		mu.Lock()
		defer mu.Unlock()
		requests++
		if !healthy {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer server.Close()
	c := testClient(t)
	if c.IsLookupdReady() {
		t.Fatal("new client should not be ready")
	}
	if err := c.WaitForLookupd(server.URL+"/", 1, time.Nanosecond); err != nil || !c.IsLookupdReady() {
		t.Fatalf("ready=%v err=%v", c.IsLookupdReady(), err)
	}
	mu.Lock()
	healthy = false
	requests = 0
	mu.Unlock()
	if err := c.WaitForLookupd(server.URL, 2, time.Nanosecond); err == nil || c.IsLookupdReady() {
		t.Fatalf("ready=%v err=%v", c.IsLookupdReady(), err)
	}
	mu.Lock()
	defer mu.Unlock()
	if requests != 2 {
		t.Fatalf("readiness attempts=%d", requests)
	}
}

func TestConsumerValidation(t *testing.T) {
	c := testClient(t)
	handler := func(*nsq.Message) error { return nil }
	for _, test := range []struct {
		topic       string
		addrs       string
		handler     nsq.HandlerFunc
		concurrency int
	}{
		{"topic", "", handler, 10},
		{"topic", "localhost:4161", nil, 10},
		{"topic", "localhost:4161", handler, 0},
		{"bad topic", "localhost:4161", handler, 10},
	} {
		if err := c.Consume(test.topic, "channel", test.addrs, test.handler, test.concurrency); err == nil {
			t.Fatalf("expected validation error: %+v", test)
		}
	}
}

func TestConsumerLifecycle(t *testing.T) {
	server := lookupServer(t, `[]`)
	c := testClient(t)
	if err := c.Consume("topic", "channel", server.URL, func(*nsq.Message) error { return nil }, 2); err != nil {
		t.Fatal(err)
	}
	if len(c.consumers) != 1 {
		t.Fatal("consumer not tracked")
	}
	stopped := c.consumers[0].StopChan
	if err := c.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-stopped:
	default:
		t.Fatal("Close returned before consumer stopped")
	}
}

type blockingProducer struct {
	started chan struct{}
	release chan struct{}
	stopped chan struct{}
}

func (p *blockingProducer) Publish(string, []byte) error {
	close(p.started)
	<-p.release
	return nil
}

func (p *blockingProducer) Stop() { close(p.stopped) }

func TestCloseWaitsForActivePublishAndIsTerminal(t *testing.T) {
	c := testClient(t)
	p := &blockingProducer{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	c.newProducer = func(string) (producer, error) { return p, nil }
	published := make(chan error, 1)
	go func() { published <- c.Publish("", "fallback:4150", "topic", nil) }()
	<-p.started
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.Close(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Close should return context error while publishing: %v", err)
	}
	select {
	case <-p.stopped:
		t.Fatal("stopped producer during active publish")
	default:
	}
	close(p.release)
	if err := <-published; err != nil {
		t.Fatal(err)
	}
	if err := c.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.stopped:
	default:
		t.Fatal("producer not stopped")
	}
	if err := c.Publish("", "fallback:4150", "topic", nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("publish after close: %v", err)
	}
	if err := c.Consume("topic", "channel", "localhost:4161", nil, 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("consume after close: %v", err)
	}
	if err := c.WaitForLookupd("localhost:4161", 1, time.Second); !errors.Is(err, ErrClosed) {
		t.Fatalf("readiness after close: %v", err)
	}
}

func TestCloseCancelsDiscovery(t *testing.T) {
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
	}))
	defer server.Close()
	c := testClient(t)
	result := make(chan error, 1)
	go func() { result <- c.Publish(server.URL, "", "topic", nil) }()
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err == nil {
		t.Fatal("expected canceled discovery error")
	}
}

func TestConcurrentPublishersShareProducer(t *testing.T) {
	c := testClient(t)
	p := &fakeProducer{}
	created := 0
	c.newProducer = func(string) (producer, error) {
		created++
		return p, nil
	}
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c.SetProducerDNSVerification(i%2 == 0)
			if err := c.Publish("", "fallback:4150", "topic", []byte("body")); err != nil {
				t.Errorf("Publish: %v", err)
			}
			c.UsageMetrics()
		}(i)
	}
	wg.Wait()
	if created != 1 || c.UsageMetrics()["fallback:4150"] != 30 {
		t.Fatalf("created=%d metrics=%v", created, c.UsageMetrics())
	}
}
