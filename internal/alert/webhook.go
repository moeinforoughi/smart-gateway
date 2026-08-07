package alert

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Webhook delivers alerts as JSON POSTs to a generic endpoint. Headers (e.g.
// an authorization token) are attached verbatim from the configuration.
type Webhook struct {
	URL     string
	Headers map[string]string
	Timeout time.Duration
	Gateway string

	// Client may be replaced for tests; defaults to a timeout-bound client.
	Client *http.Client
}

// NewWebhook builds a webhook notifier.
func NewWebhook(url string, headers map[string]string, timeout time.Duration) *Webhook {
	return &Webhook{URL: url, Headers: headers, Timeout: timeout}
}

// Name implements Notifier.
func (w *Webhook) Name() string { return "webhook" }

// Notify implements Notifier.
func (w *Webhook) Notify(ctx context.Context, a Alert) error {
	if w.URL == "" {
		return fmt.Errorf("webhook: no url configured")
	}
	payload := map[string]any{
		"severity": string(a.Severity),
		"type":     a.Type,
		"key":      a.Key,
		"title":    a.Title,
		"message":  a.Message,
		"at":       a.At.UTC().Format(time.RFC3339),
	}
	if w.Gateway != "" {
		payload["gateway"] = w.Gateway
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range w.Headers {
		req.Header.Set(k, v)
	}
	resp, err := w.client().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("webhook: unexpected HTTP %d", resp.StatusCode)
	}
	return nil
}

func (w *Webhook) client() *http.Client {
	if w.Client != nil {
		return w.Client
	}
	timeout := w.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &http.Client{Timeout: timeout}
}
