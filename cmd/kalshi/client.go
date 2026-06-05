package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const baseURL = "https://external-api.kalshi.com/trade-api/v2"

// tickerFields holds the only fields we extract from list elements across the
// series, events and markets endpoints. encoding/json ignores everything else.
type tickerFields struct {
	Ticker       string `json:"ticker"`
	EventTicker  string `json:"event_ticker"`
	SeriesTicker string `json:"series_ticker"`
}

// Client is a rate-limited HTTP client. A single shared limiter paces all
// callers to a target RPS, and a shared pause gate makes every in-flight worker
// back off together when the API returns 429.
type Client struct {
	http    *http.Client
	limiter <-chan time.Time
	ticker  *time.Ticker

	mu         sync.Mutex
	pauseUntil time.Time

	n429 int64
}

// NewClient builds a client that issues at most rps requests per second.
func NewClient(rps float64) *Client {
	if rps <= 0 {
		rps = 1
	}
	t := time.NewTicker(time.Duration(float64(time.Second) / rps))
	return &Client{
		http:    &http.Client{Timeout: 60 * time.Second},
		limiter: t.C,
		ticker:  t,
	}
}

func (c *Client) Close()         { c.ticker.Stop() }
func (c *Client) Rate429() int64 { return atomic.LoadInt64(&c.n429) }

// pause extends the shared backoff window so all workers wait at least d.
func (c *Client) pause(d time.Duration) {
	c.mu.Lock()
	if until := time.Now().Add(d); until.After(c.pauseUntil) {
		c.pauseUntil = until
	}
	c.mu.Unlock()
}

// waitPause blocks until the shared backoff window has elapsed.
func (c *Client) waitPause(ctx context.Context) {
	for {
		c.mu.Lock()
		d := time.Until(c.pauseUntil)
		c.mu.Unlock()
		if d <= 0 {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(d):
		}
	}
}

// get performs a rate-limited GET, retrying 429 indefinitely (with shared
// exponential backoff until the bucket refills) and 5xx/network errors up to a
// bounded number of attempts. Non-retryable responses are surfaced as errors.
func (c *Client) get(ctx context.Context, endpoint string, q url.Values) ([]byte, error) {
	full := baseURL + endpoint
	if len(q) > 0 {
		full += "?" + q.Encode()
	}

	const maxBackoff = 60 * time.Second
	const maxFails = 8
	backoff := time.Second
	fails := 0

	for {
		c.waitPause(ctx)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.limiter:
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, full, nil)
		if err != nil {
			return nil, err
		}
		resp, err := c.http.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			fails++
			if fails > maxFails {
				return nil, fmt.Errorf("GET %s: %w", full, err)
			}
			c.sleep(ctx, backoff)
			backoff = capDur(backoff*2, maxBackoff)
			continue
		}

		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		switch {
		case resp.StatusCode == http.StatusOK:
			return body, nil

		case resp.StatusCode == http.StatusTooManyRequests:
			n := atomic.AddInt64(&c.n429, 1)
			d := retryAfter(resp.Header, backoff)
			log.Printf("429 rate limited; pausing %s (429s so far: %d)", d.Round(time.Millisecond), n)
			c.pause(d)
			c.sleep(ctx, d)
			backoff = capDur(backoff*2, maxBackoff)
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			continue

		case resp.StatusCode >= 500:
			fails++
			if fails > maxFails {
				return nil, fmt.Errorf("GET %s: status %d: %s", full, resp.StatusCode, truncate(body))
			}
			c.sleep(ctx, backoff)
			backoff = capDur(backoff*2, maxBackoff)
			continue

		default:
			return nil, fmt.Errorf("GET %s: status %d: %s", full, resp.StatusCode, truncate(body))
		}
	}
}

// paginate fetches pages from a cursor-paginated list endpoint starting at
// startCursor, invoking onItem for every list element. onPage, if non-nil, is
// called after each page with the next cursor so callers can checkpoint. It
// follows the cursor until the listing is exhausted.
func (c *Client) paginate(
	ctx context.Context,
	endpoint string,
	params url.Values,
	listKey string,
	startCursor string,
	onItem func(tickerFields),
	onPage func(cursor string) error,
) error {
	cursor := startCursor
	for {
		q := url.Values{}
		for k, v := range params {
			q[k] = v
		}
		if cursor != "" {
			q.Set("cursor", cursor)
		}

		body, err := c.get(ctx, endpoint, q)
		if err != nil {
			return err
		}

		var top map[string]json.RawMessage
		if err := json.Unmarshal(body, &top); err != nil {
			return fmt.Errorf("decode %s: %w", endpoint, err)
		}
		var items []tickerFields
		if raw, ok := top[listKey]; ok {
			if err := json.Unmarshal(raw, &items); err != nil {
				return fmt.Errorf("decode %s[%s]: %w", endpoint, listKey, err)
			}
		}
		for _, it := range items {
			onItem(it)
		}

		cursor = ""
		if raw, ok := top["cursor"]; ok {
			_ = json.Unmarshal(raw, &cursor)
		}

		if onPage != nil {
			if err := onPage(cursor); err != nil {
				return err
			}
		}

		if cursor == "" || len(items) == 0 {
			return nil
		}
	}
}

// retryAfter honors a Retry-After header (seconds or HTTP date) when present,
// otherwise returns the jittered fallback backoff.
func retryAfter(h http.Header, fallback time.Duration) time.Duration {
	if v := h.Get("Retry-After"); v != "" {
		if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
			return time.Duration(secs) * time.Second
		}
		if t, err := http.ParseTime(v); err == nil {
			if d := time.Until(t); d > 0 {
				return d
			}
		}
	}
	return fallback + time.Duration(rand.Int63n(int64(fallback/2+1)))
}

func (c *Client) sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

func capDur(d, max time.Duration) time.Duration {
	if d > max {
		return max
	}
	return d
}

func truncate(b []byte) string {
	const n = 200
	if len(b) > n {
		return string(b[:n]) + "..."
	}
	return string(b)
}
