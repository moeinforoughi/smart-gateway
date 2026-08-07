package speed

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	"smart-gateway/internal/domain"
)

// ErrNotMeasurable reports that a node's protocol cannot be throughput-tested
// directly without going through the data plane.
var ErrNotMeasurable = errors.New("protocol requires data-plane measurement; covered by passive attribution")

// DirectMeasurer estimates raw gateway↔server bandwidth for plain HTTP and
// SOCKS proxy nodes by streaming a download from Target through the node as
// an HTTP CONNECT / SOCKS5 proxy. For obfuscated protocols (vless, vmess,
// trojan, shadowsocks, ssr, hysteria2, tuic) it returns ErrNotMeasurable —
// throughput for those is attributed passively through the data plane
// (documented approximation, FR-506).
type DirectMeasurer struct {
	target    string
	timeout   time.Duration
	size      int64
	verifyTLS bool
	clock     domain.Clock
}

// NewDirectMeasurer builds a measurer that downloads size bytes from target.
func NewDirectMeasurer(target string, timeout time.Duration, size int64, verifyTLS bool, clock domain.Clock) *DirectMeasurer {
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	if clock == nil {
		clock = domain.SystemClock{}
	}
	return &DirectMeasurer{target: target, timeout: timeout, size: size, verifyTLS: verifyTLS, clock: clock}
}

// Measure implements Measurer.
func (m *DirectMeasurer) Measure(ctx context.Context, n domain.Node, size int64) (float64, error) {
	if size <= 0 {
		size = m.size
	}
	if size <= 0 {
		size = 1 << 20
	}
	switch n.Protocol {
	case domain.ProtocolHTTP, domain.ProtocolSOCKS:
	default:
		return 0, fmt.Errorf("%w: %s", ErrNotMeasurable, n.Protocol)
	}

	proxyURL := &url.URL{
		Scheme: "http",
		Host:   net.JoinHostPort(n.Server, fmt.Sprint(n.Port)),
	}
	if n.Protocol == domain.ProtocolSOCKS {
		proxyURL.Scheme = "socks5"
	}
	if u := n.Params["username"]; u != "" {
		proxyURL.User = url.UserPassword(u, n.Params["password"])
	}

	dialer := &net.Dialer{Timeout: m.timeout, KeepAlive: 30 * time.Second}
	transport := &http.Transport{
		Proxy:               http.ProxyURL(proxyURL),
		DialContext:         dialer.DialContext,
		TLSHandshakeTimeout: m.timeout,
	}
	if !m.verifyTLS {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   m.timeout,
	}

	start := m.clock.Now()
	reqCtx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, m.target, nil)
	if err != nil {
		return 0, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("unexpected status %s", resp.Status)
	}

	read, err := io.Copy(io.Discard, io.LimitReader(resp.Body, size))
	if err != nil {
		return 0, err
	}
	if read == 0 {
		return 0, errors.New("no bytes read")
	}
	elapsed := m.clock.Now().Sub(start)
	if elapsed <= 0 {
		elapsed = time.Nanosecond
	}
	return float64(read) / elapsed.Seconds(), nil
}
