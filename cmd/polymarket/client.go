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

const baseURL = "https://gamma-api.polymarket.com"

// Client is a rate-limited HTTP client. A single shared limiter paces all
// callers to a target QPS, and a shared pause gate makes every in-flight caller
// back off together when the API returns 429.
type Client struct {
	http    *http.Client
	limiter <-chan time.Time
	ticker  *time.Ticker

	mu         sync.Mutex
	pauseUntil time.Time

	n429 int64
}

// NewClient builds a client that issues at most qps requests per second.
func NewClient(qps int) *Client {
	if qps < 1 {
		qps = 1
	}
	t := time.NewTicker(time.Second / time.Duration(qps))
	return &Client{
		http:    &http.Client{Timeout: 60 * time.Second},
		limiter: t.C,
		ticker:  t,
	}
}

func (c *Client) Close()         { c.ticker.Stop() }
func (c *Client) Rate429() int64 { return atomic.LoadInt64(&c.n429) }

// pause extends the shared backoff window so all callers wait at least d.
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

// paginate walks a keyset-paginated Gamma listing. Each response is an object
// holding the page under listKey and a next_cursor; the cursor is passed back
// as after_cursor on the following request (the offset parameter is rejected by
// these endpoints). For each page it calls handle with the raw list bytes;
// handle decodes the page and returns how many elements it held. onPage, if
// non-nil, is called after each page with the next cursor so callers can
// checkpoint. Pagination stops when next_cursor is empty, the page is empty, or
// the cursor stops advancing.
func (c *Client) paginate(
	ctx context.Context,
	endpoint string,
	params url.Values,
	limit int,
	listKey, startCursor string,
	handle func(list json.RawMessage) (count int, err error),
	onPage func(nextCursor string) error,
) error {
	cursor := startCursor
	for {
		q := url.Values{}
		for k, v := range params {
			q[k] = v
		}
		q.Set("limit", strconv.Itoa(limit))
		if cursor != "" {
			q.Set("after_cursor", cursor)
		}

		body, err := c.get(ctx, endpoint, q)
		if err != nil {
			return err
		}

		var raw map[string]json.RawMessage
		if err := json.Unmarshal(body, &raw); err != nil {
			return fmt.Errorf("decode %s response: %w", endpoint, err)
		}
		var next string
		if nc, ok := raw["next_cursor"]; ok {
			_ = json.Unmarshal(nc, &next)
		}

		n := 0
		if list := raw[listKey]; len(list) > 0 {
			n, err = handle(list)
			if err != nil {
				return fmt.Errorf("decode %s[%s]: %w", endpoint, listKey, err)
			}
		}

		if onPage != nil {
			if err := onPage(next); err != nil {
				return err
			}
		}

		if next == "" || n == 0 || next == cursor {
			return nil
		}
		cursor = next
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
