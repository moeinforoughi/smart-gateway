package singbox

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// ClashClient talks to the sing-box Clash API. It implements
// domain.OutboundApplier so routing only depends on the port.
type ClashClient struct {
	base   string
	secret string
	hc     *http.Client
}

// Connection is one active connection reported by /connections.
type Connection struct {
	ID          string    `json:"id"`
	Rule        string    `json:"rule"`
	RulePayload string    `json:"rulePayload"`
	Start       time.Time `json:"start"`
	Chains      []string  `json:"chains"`
	Upload      int64     `json:"upload"`
	Download    int64     `json:"download"`
	Metadata    struct {
		Network         string `json:"network"`
		Type            string `json:"type"`
		SourceIP        string `json:"sourceIP"`
		DestinationIP   string `json:"destinationIP"`
		Host            string `json:"host"`
		DestinationPort string `json:"destinationPort"`
	} `json:"metadata"`
}

// ConnectionsSnapshot is the parsed body of GET /connections.
type ConnectionsSnapshot struct {
	DownloadTotal int64
	UploadTotal   int64
	Connections   []Connection
}

// NewClashClient dials baseURL (http://127.0.0.1:9090) with the given secret.
func NewClashClient(baseURL, secret string, timeout time.Duration) *ClashClient {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = (&net.Dialer{
		Timeout:   5 * time.Second,
		KeepAlive: 30 * time.Second,
	}).DialContext
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &ClashClient{
		base:   strings.TrimSuffix(baseURL, "/"),
		secret: secret,
		hc:     &http.Client{Timeout: timeout, Transport: transport},
	}
}

// Close releases idle connections.
func (c *ClashClient) Close() { c.hc.CloseIdleConnections() }

func (c *ClashClient) do(ctx context.Context, method, path string, body []byte, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.secret)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode > 299 {
		return fmt.Errorf("clash api %s %s: HTTP %d", method, path, resp.StatusCode)
	}
	if out != nil {
		if err := json.Unmarshal(data, out); err != nil {
			return fmt.Errorf("clash api decode %s: %w", path, err)
		}
	}
	return nil
}

// ApplySelection switches the selector tagged `sel` to nodeID (FR-7xx no-drop
// semantics live in the selector config: interrupt_exist_connections=false).
func (c *ClashClient) ApplySelection(ctx context.Context, sel, nodeID string) error {
	if sel == "" {
		sel = DefaultSelectorTag
	}
	body, _ := json.Marshal(map[string]string{"name": nodeID})
	return c.do(ctx, http.MethodPut, "/proxies/"+url.PathEscape(sel), body, nil)
}

// Current returns the active member of the selector tagged sel.
func (c *ClashClient) Current(ctx context.Context, sel string) (string, error) {
	if sel == "" {
		sel = DefaultSelectorTag
	}
	var out struct {
		Now string `json:"now"`
	}
	if err := c.do(ctx, http.MethodGet, "/proxies/"+url.PathEscape(sel), nil, &out); err != nil {
		return "", err
	}
	return out.Now, nil
}

// Delay runs a deep delay probe through the outbound tagged nodeID
// (GET /proxies/{tag}/delay).
func (c *ClashClient) Delay(ctx context.Context, nodeID string, timeout time.Duration) (time.Duration, error) {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	q := url.Values{}
	q.Set("timeout", strconv.Itoa(int(timeout.Milliseconds())))
	q.Set("url", "https://www.gstatic.com/generate_204")
	var out struct {
		Delay int64 `json:"delay"`
	}
	if err := c.do(ctx, http.MethodGet, "/proxies/"+url.PathEscape(nodeID)+"/delay?"+q.Encode(), nil, &out); err != nil {
		return 0, err
	}
	return time.Duration(out.Delay) * time.Millisecond, nil
}

// Connections snapshots /connections once.
func (c *ClashClient) Connections(ctx context.Context) (*ConnectionsSnapshot, error) {
	var out struct {
		DownloadTotal int64        `json:"downloadTotal"`
		UploadTotal   int64        `json:"uploadTotal"`
		Connections   []Connection `json:"connections"`
	}
	if err := c.do(ctx, http.MethodGet, "/connections", nil, &out); err != nil {
		return nil, err
	}
	return &ConnectionsSnapshot{
		DownloadTotal: out.DownloadTotal,
		UploadTotal:   out.UploadTotal,
		Connections:   out.Connections,
	}, nil
}

// Traffic samples the /traffic SSE stream for up to sampleFor and returns the
// last cumulative up/down byte counters (zero when nothing is flowing).
func (c *ClashClient) Traffic(ctx context.Context, sampleFor time.Duration) (up, down int64, err error) {
	if sampleFor <= 0 {
		sampleFor = 2 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, sampleFor+time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/traffic", nil)
	if err != nil {
		return 0, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+c.secret)
	resp, err := c.hc.Do(req)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, 0, fmt.Errorf("clash api GET /traffic: HTTP %d", resp.StatusCode)
	}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 4096), 64<<10)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		var ev struct {
			Up   int64 `json:"up"`
			Down int64 `json:"down"`
		}
		if json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data:"))), &ev) == nil {
			up, down = ev.Up, ev.Down
		}
	}
	if err := sc.Err(); err != nil && !errors.Is(err, io.EOF) && !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return 0, 0, err
	}
	return up, down, nil
}
