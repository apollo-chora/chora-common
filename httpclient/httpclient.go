// Package httpclient provides a pre-configured HTTP client for Chora services
// calling other Chora services or external APIs. Features:
//
//   - Timeout (default 10s)
//   - Bounded retries on 5xx with exponential backoff
//   - Automatic W3C traceparent injection from context
//   - Context cancellation support
//   - Base URL composition (callers supply path-only routes)
//
// Per CLAUDE.md §6 ("No inline config"), the base URL MUST come from env vars
// at the call site — this client refuses construction with empty baseURL.
package httpclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/5007-Capstone/chora/libs/chora-go-common/tracing"
)

// Client is a Chora-flavoured HTTP client.
type Client struct {
	baseURL      string
	httpClient   *http.Client
	maxRetries   int
	retryBackoff time.Duration
}

// Option mutates a Client during New construction.
type Option func(*Client)

// WithTimeout sets the per-request timeout (default 10s).
func WithTimeout(d time.Duration) Option {
	return func(c *Client) { c.httpClient.Timeout = d }
}

// WithMaxRetries caps the number of retries on retryable failures (5xx, network).
// 0 disables retries (default 2).
func WithMaxRetries(n int) Option {
	return func(c *Client) {
		if n < 0 {
			n = 0
		}
		c.maxRetries = n
	}
}

// WithRetryBackoff sets the initial backoff between retries; doubles each attempt.
func WithRetryBackoff(d time.Duration) Option {
	return func(c *Client) { c.retryBackoff = d }
}

// WithHTTPClient injects a custom *http.Client (e.g. for mTLS or test transport).
func WithHTTPClient(h *http.Client) Option {
	return func(c *Client) { c.httpClient = h }
}

// New constructs a Client. baseURL must be non-empty (config-from-env discipline).
func New(baseURL string, opts ...Option) (*Client, error) {
	if baseURL == "" {
		return nil, fmt.Errorf("httpclient: baseURL is required (no inline config — read from env)")
	}
	if _, err := url.Parse(baseURL); err != nil {
		return nil, fmt.Errorf("httpclient: invalid baseURL %q: %w", baseURL, err)
	}
	c := &Client{
		baseURL:      strings.TrimRight(baseURL, "/"),
		httpClient:   &http.Client{Timeout: 10 * time.Second},
		maxRetries:   2,
		retryBackoff: 100 * time.Millisecond,
	}
	for _, o := range opts {
		o(c)
	}
	return c, nil
}

// Get issues a GET against the path joined onto baseURL. Caller owns
// resp.Body.Close().
func (c *Client) Get(ctx context.Context, path string) (*http.Response, error) {
	return c.do(ctx, http.MethodGet, path, nil)
}

// Post issues a POST with the supplied body.
func (c *Client) Post(ctx context.Context, path string, body io.Reader) (*http.Response, error) {
	return c.do(ctx, http.MethodPost, path, body)
}

// do is the inner request loop with retry + traceparent stamping.
func (c *Client) do(ctx context.Context, method, path string, body io.Reader) (*http.Response, error) {
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	target := c.baseURL + path

	var lastErr error
	backoff := c.retryBackoff
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		req, err := http.NewRequestWithContext(ctx, method, target, body)
		if err != nil {
			return nil, fmt.Errorf("httpclient: build request: %w", err)
		}
		tracing.Inject(req)

		resp, err := c.httpClient.Do(req)
		if err != nil {
			// Context cancellation / timeout is terminal — do not retry.
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil, err
			}
			lastErr = err
			if attempt < c.maxRetries {
				if !sleepCtx(ctx, backoff) {
					return nil, ctx.Err()
				}
				backoff *= 2
				continue
			}
			return nil, fmt.Errorf("httpclient: %s %s: %w", method, target, err)
		}

		// Retryable status codes: 5xx only. 4xx is client error — caller decides.
		if resp.StatusCode >= 500 && attempt < c.maxRetries {
			_ = resp.Body.Close()
			lastErr = fmt.Errorf("httpclient: server returned %d", resp.StatusCode)
			if !sleepCtx(ctx, backoff) {
				return nil, ctx.Err()
			}
			backoff *= 2
			continue
		}
		return resp, nil
	}
	return nil, lastErr
}

// sleepCtx sleeps for d unless ctx cancels first. Returns false on ctx done.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
