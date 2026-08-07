package health

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"smart-gateway/internal/domain"
)

type fakeClock struct {
	mu atomic.Int64 // unix nanos
}

func (f *fakeClock) Now() time.Time          { return time.Unix(0, f.mu.Load()) }
func (f *fakeClock) Set(t time.Time)         { f.mu.Store(t.UnixNano()) }
func (f *fakeClock) Advance(d time.Duration) { f.mu.Add(int64(d)) }

func probeCfg() ProbeConfig {
	return ProbeConfig{Timeout: time.Second, HTTPProbe: false, VerifyTLS: false}
}

func TestDirectProberHysteries2Unsupported(t *testing.T) {
	p := NewDirectProber(probeCfg(), nil, nil)
	res := p.Probe(context.Background(), domain.Node{
		Protocol: domain.ProtocolHysteria2, Server: "127.0.0.1", Port: 443,
	})
	if res.OK {
		t.Fatal("QUIC node must not be considered reachable by direct probe")
	}
	if res.Stage != StageUnsupported {
		t.Fatalf("Stage = %s, want unsupported", res.Stage)
	}
}

func TestDirectProberMissingEndpoint(t *testing.T) {
	p := NewDirectProber(probeCfg(), nil, nil)
	res := p.Probe(context.Background(), domain.Node{Protocol: domain.ProtocolVless})
	if res.OK {
		t.Fatal("node without server/port must fail")
	}
	if res.Stage != StageDial {
		t.Fatalf("Stage = %s, want dial", res.Stage)
	}
}

func TestDirectProberConnectFailure(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().(*net.TCPAddr)
	_ = l.Close() // closed port -> connection refused

	p := NewDirectProber(probeCfg(), nil, nil)
	res := p.Probe(context.Background(), domain.Node{
		Protocol: domain.ProtocolVless, Server: "127.0.0.1", Port: addr.Port,
	})
	if res.OK {
		t.Fatal("connection to a closed port must fail")
	}
	if res.Stage != StageDial {
		t.Fatalf("Stage = %s, want dial", res.Stage)
	}
}

func TestDirectProberTCPOnlyReachable(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	addr := l.Addr().(*net.TCPAddr)
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()

	clk := &stepClock{}
	p := NewDirectProber(probeCfg(), clk, nil)
	res := p.Probe(context.Background(), domain.Node{
		Protocol: domain.ProtocolVless, Server: "127.0.0.1", Port: addr.Port,
	})
	if !res.OK {
		t.Fatalf("probe should succeed, got stage %s: %s", res.Stage, res.Detail)
	}
	if res.Latency <= 0 {
		t.Fatalf("Latency = %v, want > 0", res.Latency)
	}
}

// stepClock advances 1ms per call so elapsed times are measurable on Windows,
// where time.Now() granularity can collapse sub-millisecond durations to zero.
type stepClock struct {
	n int64
}

func (s *stepClock) Now() time.Time {
	s.n++
	return time.Unix(0, s.n*int64(time.Millisecond))
}

func TestDirectProberTLSPathVerified(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	// VerifyTLS=false so the self-signed cert is accepted; we only assert the
	// handshake runs on the TLS path (stage must not be "tls").
	p := NewDirectProber(ProbeConfig{Timeout: time.Second, HTTPProbe: false, VerifyTLS: false}, nil, nil)
	host, portStr, _ := net.SplitHostPort(srv.Listener.Addr().String())
	port, _ := strconv.Atoi(portStr)
	res := p.Probe(context.Background(), domain.Node{
		Protocol: domain.ProtocolTrojan, Server: host, Port: port, TLS: true, SNI: "localhost",
	})
	if !res.OK {
		t.Fatalf("TLS handshake should pass with verify disabled, got %s: %s", res.Stage, res.Detail)
	}
}

func TestDirectProberHTTPStage(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	host, portStr, _ := net.SplitHostPort(srv.Listener.Addr().String())
	port, _ := strconv.Atoi(portStr)

	p := NewDirectProber(ProbeConfig{Timeout: time.Second, HTTPProbe: true}, nil, nil)
	res := p.Probe(context.Background(), domain.Node{
		Protocol: domain.ProtocolHTTP, Server: host, Port: port, Transport: "tcp",
	})
	if !res.OK {
		t.Fatalf("HTTP probe should succeed, got %s: %s", res.Stage, res.Detail)
	}
}

func TestDirectProberHTTPStageBadStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()
	host, portStr, _ := net.SplitHostPort(srv.Listener.Addr().String())
	port, _ := strconv.Atoi(portStr)

	p := NewDirectProber(ProbeConfig{Timeout: time.Second, HTTPProbe: true}, nil, nil)
	res := p.Probe(context.Background(), domain.Node{
		Protocol: domain.ProtocolHTTP, Server: host, Port: port, Transport: "tcp",
	})
	if res.OK {
		t.Fatal("404 must fail the HTTP stage")
	}
	if res.Stage != StageHTTP {
		t.Fatalf("Stage = %s, want http", res.Stage)
	}
}
