// Package nsqcommon provides NSQ transport without application message schemas.
// Staged here for github.com/m-theory-io/nsq-common: remove the build constraint
// above when moving this file into that module.
package nsqcommon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nsqio/go-nsq"
)

var ErrClosed = errors.New("NSQ client is closed")

// Config controls publisher behavior. Nonpositive delays use 100ms for producer
// creation retries and 200ms for publish retries. Each operation tries at most
// three times, with linear backoff, against the same selected broker.
type Config struct {
	VerifyProducerDNS  bool
	CreationRetryDelay time.Duration
	PublishRetryDelay  time.Duration
}

type producer interface {
	Publish(string, []byte) error
	Stop()
}

// Client owns its producers, consumers, and metrics. Use NewClient to create
// one, and Close during shutdown. A closed client cannot be restarted.
type Client struct {
	mu           sync.Mutex
	config       Config
	producers    map[string]producer
	consumers    []*nsq.Consumer
	roundRobin   map[string]int
	metrics      map[string]int64
	total        int64
	lookupdReady bool
	closed       bool
	active       sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
	done         chan struct{}
	httpClient   *http.Client
	newProducer  func(string) (producer, error)
	resolveHost  func(context.Context, string) bool
}

func NewClient(config Config) *Client {
	if config.CreationRetryDelay <= 0 {
		config.CreationRetryDelay = 100 * time.Millisecond
	}
	if config.PublishRetryDelay <= 0 {
		config.PublishRetryDelay = 200 * time.Millisecond
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Client{
		config: config, producers: make(map[string]producer),
		roundRobin: make(map[string]int), metrics: make(map[string]int64),
		ctx: ctx, cancel: cancel, done: make(chan struct{}),
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
			Transport: &http.Transport{
				Proxy:           http.ProxyFromEnvironment,
				DialContext:     (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
				IdleConnTimeout: 90 * time.Second,
			},
		},
		newProducer: func(addr string) (producer, error) {
			config := nsq.NewConfig()
			config.DialTimeout = 10 * time.Second
			config.WriteTimeout = 10 * time.Second
			config.ReadTimeout = 60 * time.Second
			return nsq.NewProducer(addr, config)
		},
		resolveHost: func(ctx context.Context, host string) bool {
			if net.ParseIP(host) != nil || host == "localhost" {
				return true
			}
			ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
			return err == nil && len(ips) > 0
		},
	}
}

func (c *Client) begin() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	c.active.Add(1)
	return nil
}

// SetProducerDNSVerification filters discovered hosts by DNS resolution, not
// broker health. If none resolve, a configured fallback is used; otherwise the
// original list is retained, matching the existing DGS discovery policy.
func (c *Client) SetProducerDNSVerification(verify bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.config.VerifyProducerDNS = verify
}

// PublishJSON serializes value directly, without adding an envelope or IDs.
// Use json.RawMessage for pre-encoded JSON, or Publish for arbitrary bytes.
func (c *Client) PublishJSON(lookupdAddrs, fallback, topic string, value interface{}) error {
	body, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal NSQ message: %w", err)
	}
	return c.Publish(lookupdAddrs, fallback, topic, body)
}

// Publish retries ambiguous transport failures; an acknowledgment can be lost
// after delivery, so consumers must tolerate duplicate messages. In-flight NSQ
// calls use the producer timeouts; Close cancels discovery and retry waits.
func (c *Client) Publish(lookupdAddrs, fallback, topic string, body []byte) error {
	if err := c.begin(); err != nil {
		return err
	}
	defer c.active.Done()
	if !nsq.IsValidTopicName(topic) {
		return fmt.Errorf("invalid NSQ topic %q", topic)
	}

	addr, err := c.selectAddress(lookupdAddrs, fallback, topic)
	if err != nil {
		return err
	}
	for attempt := 1; attempt <= 3; attempt++ {
		if c.ctx.Err() != nil {
			return ErrClosed
		}
		p, createErr := c.getOrCreateProducer(addr)
		delay := c.config.CreationRetryDelay
		err = createErr
		if createErr == nil {
			err = p.Publish(topic, body)
			delay = c.config.PublishRetryDelay
			if err == nil {
				c.recordUsage(addr)
				log.Printf("[NSQ] Published topic=%s nsqd=%s attempt=%d", topic, addr, attempt)
				return nil
			}
			var networkErr net.Error
			if errors.As(err, &networkErr) || errors.Is(err, nsq.ErrNotConnected) ||
				errors.Is(err, nsq.ErrStopped) || errors.Is(err, nsq.ErrClosing) {
				c.retireProducer(addr, p)
			}
		}
		log.Printf("[NSQ] Publish failed topic=%s nsqd=%s attempt=%d/3: %v", topic, addr, attempt, err)
		if attempt < 3 {
			if waitErr := c.wait(time.Duration(attempt) * delay); waitErr != nil {
				return waitErr
			}
		}
	}
	return fmt.Errorf("publish topic=%s nsqd=%s failed after 3 attempts: %w", topic, addr, err)
}

func (c *Client) getOrCreateProducer(addr string) (producer, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, ErrClosed
	}
	if p := c.producers[addr]; p != nil {
		return p, nil
	}
	p, err := c.newProducer(addr)
	if err != nil {
		return nil, err
	}
	c.producers[addr] = p
	return p, nil
}

func (c *Client) retireProducer(addr string, p producer) {
	c.mu.Lock()
	if c.producers[addr] == p {
		delete(c.producers, addr)
	}
	c.mu.Unlock()
	p.Stop()
}

func splitAddresses(addrs string) []string {
	var result []string
	for _, addr := range strings.Split(addrs, ",") {
		if addr = strings.TrimSpace(addr); addr != "" {
			result = append(result, addr)
		}
	}
	return result
}

func lookupdURL(addr, path string) string {
	if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
		addr = "http://" + addr
	}
	return strings.TrimRight(addr, "/") + path
}

func (c *Client) getJSON(endpoint string, result interface{}) error {
	req, err := http.NewRequestWithContext(c.ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s returned status %d", endpoint, resp.StatusCode)
	}
	if result == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(result)
}

func (c *Client) discover(addrs, topic string) ([]string, error) {
	var lastErr error
	for _, addr := range splitAddresses(addrs) {
		var result struct {
			Producers []struct {
				Host string `json:"broadcast_address"`
				Port int    `json:"tcp_port"`
			} `json:"producers"`
		}
		err := c.getJSON(lookupdURL(addr, "/lookup?topic="+url.QueryEscape(topic)), &result)
		if err == nil && len(result.Producers) > 0 {
			producers := make([]string, 0, len(result.Producers))
			for _, p := range result.Producers {
				if p.Host == "" || p.Port < 1 || p.Port > 65535 {
					log.Printf("[NSQ] Ignoring invalid discovered producer host=%q port=%d", p.Host, p.Port)
					continue
				}
				producers = append(producers, net.JoinHostPort(p.Host, strconv.Itoa(p.Port)))
			}
			if len(producers) > 0 {
				return producers, nil
			}
		}
		if err == nil {
			err = fmt.Errorf("no producers for topic %s", topic)
		}
		lastErr = err
		log.Printf("[NSQ] Discovery failed lookupd=%s: %v", addr, err)
	}
	if lastErr == nil {
		lastErr = errors.New("no lookupd addresses configured")
	}
	return nil, lastErr
}

func (c *Client) selectAddress(addrs, fallback, topic string) (string, error) {
	list, err := c.discover(addrs, topic)
	if err != nil {
		if fallback == "" {
			return "", fmt.Errorf("no nsqd address available: %w", err)
		}
		log.Printf("[NSQ] Using fallback nsqd=%s: %v", fallback, err)
		return fallback, nil
	}
	c.mu.Lock()
	verify := c.config.VerifyProducerDNS
	c.mu.Unlock()
	if verify {
		filtered := make([]string, 0, len(list))
		for _, addr := range list {
			host, _, err := net.SplitHostPort(addr)
			if err == nil && c.resolveHost(c.ctx, host) {
				filtered = append(filtered, addr)
			} else {
				log.Printf("[NSQ] Excluding unresolved producer %s", addr)
			}
		}
		if len(filtered) > 0 {
			list = filtered
		} else if fallback != "" {
			log.Printf("[NSQ] No discovered hosts resolve; using fallback %s", fallback)
			return fallback, nil
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	idx := c.roundRobin[topic] % len(list)
	c.roundRobin[topic] = (idx + 1) % len(list)
	return list[idx], nil
}

func (c *Client) recordUsage(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.metrics[addr]++
	c.total++
	if c.total%100 == 0 {
		log.Printf("[NSQ Metrics] Total messages: %d Distribution: %v", c.total, c.metrics)
	}
}

// UsageMetrics counts acknowledged publishes, not end-to-end processing.
func (c *Client) UsageMetrics() map[string]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make(map[string]int64, len(c.metrics))
	for addr, count := range c.metrics {
		result[addr] = count
	}
	return result
}

func (c *Client) ResetUsageMetrics() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.metrics = make(map[string]int64)
	c.total = 0
}

// WaitForLookupd checks whether any lookupd answers /ping with HTTP 200.
// It does not check broker connectivity or continuously monitor health.
func (c *Client) WaitForLookupd(addrs string, maxRetries int, initialDelay time.Duration) error {
	if err := c.begin(); err != nil {
		return err
	}
	defer c.active.Done()
	addresses := splitAddresses(addrs)
	if len(addresses) == 0 || maxRetries < 1 || initialDelay <= 0 {
		return errors.New("lookupd addresses, positive retry count and delay are required")
	}
	c.mu.Lock()
	c.lookupdReady = false
	c.mu.Unlock()
	delay := initialDelay
	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		for _, addr := range addresses {
			if err := c.getJSON(lookupdURL(addr, "/ping"), nil); err != nil {
				lastErr = err
				log.Printf("[NSQ] Lookupd not ready addr=%s attempt=%d: %v", addr, attempt, err)
			} else {
				c.mu.Lock()
				closed := c.closed
				c.lookupdReady = !closed
				c.mu.Unlock()
				if closed {
					return ErrClosed
				}
				return nil
			}
		}
		if attempt < maxRetries {
			if delay > 30*time.Second {
				delay = 30 * time.Second
			}
			if err := c.wait(delay); err != nil {
				return err
			}
			if delay < 30*time.Second {
				delay *= 2
			}
		}
	}
	return fmt.Errorf("lookupd not ready after %d attempts: %w", maxRetries, lastErr)
}

func (c *Client) IsLookupdReady() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lookupdReady
}

func (c *Client) wait(delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-c.ctx.Done():
		return ErrClosed
	}
}

// Consume registers a consumer with all configured lookupd endpoints. A valid
// but offline lookupd is polled in the background by go-nsq. This returns setup
// errors, not an assertion that any broker connection is currently healthy.
func (c *Client) Consume(topic, channel, lookupdAddrs string, handler nsq.HandlerFunc, concurrency int) error {
	if err := c.begin(); err != nil {
		return err
	}
	defer c.active.Done()
	addresses := splitAddresses(lookupdAddrs)
	if concurrency < 1 || handler == nil || len(addresses) == 0 {
		return errors.New("consumer requires a handler, positive concurrency and lookupd addresses")
	}
	config := nsq.NewConfig()
	config.MaxAttempts = 3
	config.MaxInFlight = concurrency
	config.DialTimeout = 10 * time.Second
	config.ReadTimeout = 60 * time.Second
	config.WriteTimeout = 10 * time.Second
	config.MsgTimeout = 60 * time.Second
	config.HeartbeatInterval = 30 * time.Second
	config.LookupdPollInterval = 5 * time.Minute
	consumer, err := nsq.NewConsumer(topic, channel, config)
	if err != nil {
		return fmt.Errorf("create NSQ consumer: %w", err)
	}
	consumer.AddConcurrentHandlers(handler, concurrency)
	registered := false
	var lastErr error
	for _, addr := range addresses {
		if err := consumer.ConnectToNSQLookupd(addr); err != nil {
			lastErr = err
			log.Printf("[NSQ] Cannot register lookupd=%s topic=%s channel=%s: %v", addr, topic, channel, err)
		} else {
			registered = true
		}
	}
	if !registered {
		consumer.Stop()
		return fmt.Errorf("no lookupd endpoints registered: %w", lastErr)
	}
	c.mu.Lock()
	c.consumers = append(c.consumers, consumer)
	c.mu.Unlock()
	return nil
}

// Close is terminal and idempotent. Cleanup continues if ctx expires. New work
// is rejected immediately; active calls finish before resources are stopped.
func (c *Client) Close(ctx context.Context) error {
	c.mu.Lock()
	if !c.closed {
		c.closed = true
		c.lookupdReady = false
		c.cancel()
		go c.closeResources()
	}
	c.mu.Unlock()
	select {
	case <-c.done:
		return nil
	default:
	}
	select {
	case <-c.done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("close NSQ client: %w", ctx.Err())
	}
}

func (c *Client) closeResources() {
	c.active.Wait()
	c.mu.Lock()
	consumers, producers := c.consumers, c.producers
	c.consumers = nil
	c.producers = make(map[string]producer)
	c.mu.Unlock()
	for _, consumer := range consumers {
		consumer.Stop()
	}
	var wg sync.WaitGroup
	for _, p := range producers {
		wg.Add(1)
		go func(p producer) {
			defer wg.Done()
			p.Stop()
		}(p)
	}
	for _, consumer := range consumers {
		<-consumer.StopChan
	}
	wg.Wait()
	c.httpClient.CloseIdleConnections()
	close(c.done)
}
