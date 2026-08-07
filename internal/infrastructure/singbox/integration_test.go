// Integration test for the sing-box supervisor. Gated on SG_INTEGRATION=1 and
// a real sing-box binary (SG_SINGBOX_BIN or on PATH) because the CI sandbox
// has no unrestricted network/mirror access to fetch it. Run locally:
//
//	SG_INTEGRATION=1 SG_SINGBOX_BIN=/path/to/sing-box go test -count=1 -run Integration ./internal/infrastructure/singbox
package singbox

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"smart-gateway/internal/domain"
	"smart-gateway/internal/log"
)

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func singboxBin(t *testing.T) string {
	t.Helper()
	if p := os.Getenv("SG_SINGBOX_BIN"); p != "" {
		return p
	}
	for _, dir := range []string{"/usr/local/bin", "/usr/bin", "/opt/homebrew/bin"} {
		p := filepath.Join(dir, "sing-box")
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return "sing-box" // rely on PATH
}

func TestSupervisorIntegration(t *testing.T) {
	if os.Getenv("SG_INTEGRATION") != "1" {
		t.Skip("set SG_INTEGRATION=1 to run sing-box integration tests")
	}
	dir := t.TempDir()
	clashPort := freePort(t)
	inboundPort := freePort(t)
	in := map[string]any{
		"type":        "http",
		"tag":         "in-test",
		"listen":      "127.0.0.1",
		"listen_port": inboundPort,
		"users":       []map[string]any{{"username": "u", "password": "p"}},
	}
	sup := NewSupervisor(SupervisorOptions{
		BinaryPath:     singboxBin(t),
		ConfigDir:      dir,
		CacheFile:      filepath.Join(dir, "cache.db"),
		ClashAPIListen: "127.0.0.1:" + strconv.Itoa(clashPort),
		ClashAPISecret: "sekret",
		Inbounds:       []map[string]any{in},
		RestartMin:     time.Second,
		RestartMax:     2 * time.Second,
		RestartLimit:   5,
	}, log.New(os.Stderr, 0), nil, nil)
	defer sup.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := sup.Start(ctx); err != nil {
		t.Fatalf("start (no nodes): %v", err)
	}
	if sup.Running() {
		t.Fatal("data plane should stay down with no nodes")
	}

	nodes := []domain.Node{
		{ID: "node-a", Protocol: domain.ProtocolVless, Server: "1.1.1.1", Port: 443, Params: map[string]string{"uuid": "00000000-0000-4000-8000-000000000001"}},
		{ID: "node-b", Protocol: domain.ProtocolVless, Server: "8.8.8.8", Port: 443, Params: map[string]string{"uuid": "00000000-0000-4000-8000-000000000002"}},
	}
	if err := sup.Apply(ctx, nodes); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if !sup.Running() {
		t.Fatal("data plane should be running after apply")
	}

	waitFor(t, ctx, func() bool {
		_, err := sup.Current(ctx)
		return err == nil
	}, "clash api reachable")

	if err := sup.ApplySelection(ctx, "node-b"); err != nil {
		t.Fatalf("apply selection: %v", err)
	}
	waitFor(t, ctx, func() bool {
		cur, err := sup.Current(ctx)
		return err == nil && cur == "node-b"
	}, "selection = node-b")

	if err := sup.Stop(ctx); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if sup.Running() {
		t.Fatal("still running after stop")
	}
}

func waitFor(t *testing.T, ctx context.Context, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timeout waiting for %s", what)
		default:
			time.Sleep(200 * time.Millisecond)
		}
	}
	t.Fatalf("timeout waiting for %s", what)
}
