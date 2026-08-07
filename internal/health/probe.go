// Package health implements the health & speed monitoring pipelines (M3,
// FR-F/G, NFR-060…066): direct probes (DNS → TCP → TLS → HTTP), deep probes
// through the sing-box data plane, worker-pool isolation, bandwidth budgets,
// randomized scheduling and passive traffic attribution.
package health

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"smart-gateway/internal/domain"
	"smart-gateway/internal/log"
)

// Stage names a probe failure point (FR-407, FR-410).
type Stage string

const (
	StageResolve     Stage = "resolve"
	StageDial        Stage = "dial"
	StageTLS         Stage = "tls"
	StageHTTP        Stage = "http"
	StageUnsupported Stage = "unsupported"
)

// Result is the outcome of one direct probe.
type Result struct {
	OK      bool
	Latency time.Duration
	Stage   Stage
	Detail  string
}

// Prober measures a node's direct reachability. Implementations must honor
// the context deadline (stuck-worker isolation, FR-406).
type Prober interface {
	Probe(ctx context.Context, n domain.Node) Result
}

// ProbeConfig tunes direct probing (FR-403…405).
type ProbeConfig struct {
	Timeout   time.Duration // per-probe budget (default: 3s)
	HTTPProbe bool          // perform an HTTP round-trip after TCP/TLS (default: false)
	TargetURL string        // only used when HTTPProbe is set
	VerifyTLS bool          // verify TLS certificates on handshake (default: true)
}

// DirectProber implements the DNS → TCP → TLS → HTTP pipeline. The HTTP stage
// is opt-in because most nodes speak an obfuscated protocol where a plain
// HTTP round-trip is meaningless; TCP+TLS reachability is the routine probe.
type DirectProber struct {
	cfg    ProbeConfig
	clock  domain.Clock
	logger *log.Logger
}

// NewDirectProber builds a direct prober with defaults applied.
func NewDirectProber(cfg ProbeConfig, clock domain.Clock, logger *log.Logger) *DirectProber {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 3 * time.Second
	}
	if cfg.TargetURL == "" {
		cfg.TargetURL = "https://www.gstatic.com/generate_204"
	}
	if clock == nil {
		clock = domain.SystemClock{}
	}
	if logger == nil {
		logger = log.New(nil, 0)
	}
	return &DirectProber{cfg: cfg, clock: clock, logger: logger}
}

// Probe runs the full direct pipeline for a node and reports latency. QUIC
// nodes (hysteria2/tuic) are reported as unsupported: they need a deep probe
// through the data plane (no TCP listener).
func (p *DirectProber) Probe(ctx context.Context, n domain.Node) Result {
	if needsDeep(n.Protocol) {
		return Result{OK: false, Stage: StageUnsupported, Detail: "quic: deep probe required"}
	}
	if n.Server == "" || n.Port < 1 {
		return Result{OK: false, Stage: StageDial, Detail: "missing server or port"}
	}
	start := p.clock.Now()
	probeCtx, cancel := context.WithTimeout(ctx, p.cfg.Timeout)
	defer cancel()

	ips, err := resolve(probeCtx, n.Server, p.cfg.Timeout)
	if err != nil {
		return p.fail(StageResolve, err, start)
	}
	addr := net.JoinHostPort(ips[0].String(), fmt.Sprintf("%d", n.Port))

	conn, err := dialTCP(probeCtx, addr, p.cfg.Timeout)
	if err != nil {
		return p.fail(StageDial, err, start)
	}
	defer conn.Close()

	// TLS stage: only when the node advertises a TLS wire path (obfuscated
	// protocols, or HTTP proxies speaking https). Verifies the SNI by default.
	if n.TLS {
		if err := handshakeTLS(probeCtx, conn, n.SNI, p.cfg.VerifyTLS, p.cfg.Timeout); err != nil {
			return p.fail(StageTLS, err, start)
		}
	}
	if p.cfg.HTTPProbe {
		status, err := httpCheck(probeCtx, conn, p.cfg.TargetURL, p.cfg.Timeout)
		if err != nil {
			return p.fail(StageHTTP, err, start)
		}
		if status != http.StatusOK && status != http.StatusNoContent {
			return p.fail(StageHTTP, fmt.Errorf("unexpected HTTP %d", status), start)
		}
	}
	latency := p.clock.Now().Sub(start)
	return Result{OK: true, Latency: latency, Stage: ""}
}

func (p *DirectProber) fail(stage Stage, err error, start time.Time) Result {
	return Result{OK: false, Latency: p.clock.Now().Sub(start), Stage: stage, Detail: err.Error()}
}

// needsDeep reports whether a protocol must be probed through the data plane.
func needsDeep(p domain.Protocol) bool {
	switch p {
	case domain.ProtocolHysteria2, domain.ProtocolTuic:
		return true
	}
	return false
}

// resolve resolves host to IPs, honoring the deadline (FR-407).
func resolve(ctx context.Context, host string, timeout time.Duration) ([]net.IP, error) {
	if ip := net.ParseIP(host); ip != nil {
		return []net.IP{ip}, nil
	}
	r := &net.Resolver{}
	ips, err := r.LookupIP(ctx, "ip", host)
	if err != nil {
		return nil, err
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("no addresses for %s", host)
	}
	return ips, nil
}

// dialTCP opens a TCP connection, honoring the deadline.
func dialTCP(ctx context.Context, addr string, timeout time.Duration) (net.Conn, error) {
	d := net.Dialer{Timeout: timeout}
	return d.DialContext(ctx, "tcp", addr)
}

// handshakeTLS upgrades conn and performs the handshake with the given SNI.
func handshakeTLS(ctx context.Context, conn net.Conn, serverName string, verify bool, timeout time.Duration) (err error) {
	host := serverName
	if host == "" {
		if h, _, e := net.SplitHostPort(conn.RemoteAddr().String()); e == nil {
			host = h
		}
	}
	tc := tls.Client(conn, &tls.Config{
		ServerName:         host,
		InsecureSkipVerify: !verify, // explicit opt-out only (NFR-145)
		MinVersion:         tls.VersionTLS12,
	})
	_ = tc.SetDeadline(time.Now().Add(timeout))
	defer func() {
		_ = tc.SetDeadline(time.Time{})
	}()
	return tc.HandshakeContext(ctx)
}

// httpCheck performs a generate_204 round-trip over an established conn and
// returns the status code, draining (not reading) the small body.
func httpCheck(ctx context.Context, conn net.Conn, target string, timeout time.Duration) (int, error) {
	u, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return 0, err
	}
	host := u.Host
	if h, _, e := net.SplitHostPort(conn.RemoteAddr().String()); e == nil {
		host = h
	}
	req := &http.Request{
		Method: http.MethodGet,
		URL:    u.URL,
		Host:   host,
		Header: http.Header{"User-Agent": []string{"SmartGateway/1.0"}},
	}
	_ = conn.SetDeadline(time.Now().Add(timeout))
	defer func() { _ = conn.SetDeadline(time.Time{}) }()
	if err := req.Write(conn); err != nil {
		return 0, err
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	return resp.StatusCode, nil
}
