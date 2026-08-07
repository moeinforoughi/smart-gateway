package parser

import (
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"smart-gateway/internal/domain"
)

type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time { return c.t }

func parse(t *testing.T, content string) []domain.Node {
	t.Helper()
	nodes, errs := New().Parse([]byte(content), "sub-1", fixedClock{t: time.Unix(1_700_000_000, 0).UTC()})
	if len(errs) > 0 {
		t.Fatalf("unexpected parse errors: %v", errs)
	}
	return nodes
}

func base64Std(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

func TestParseVless(t *testing.T) {
	nodes := parse(t, `vless://11111111-1111-1111-1111-111111111111@203.0.113.1:443?encryption=none&security=reality&flow=xtls-rprx-vision&sni=www.microsoft.com&fp=chrome&type=tcp&sid=abc#US-1`)
	if len(nodes) != 1 {
		t.Fatalf("got %d nodes", len(nodes))
	}
	n := nodes[0]
	if n.Protocol != domain.ProtocolVless || n.Server != "203.0.113.1" || n.Port != 443 {
		t.Fatalf("node = %+v", n)
	}
	if n.Name != "US-1" {
		t.Fatalf("name = %q, want US-1", n.Name)
	}
	if n.TLS != true || n.SNI != "www.microsoft.com" || n.Flow != "xtls-rprx-vision" {
		t.Fatalf("vless tls/sni/flow = %+v", n)
	}
	if n.DedupeHash == "" {
		t.Fatal("dedupe hash missing")
	}
}

func TestParseVmess(t *testing.T) {
	vm := struct {
		V    string `json:"v"`
		Ps   string `json:"ps"`
		Add  string `json:"add"`
		Port string `json:"port"`
		ID   string `json:"id"`
		Aid  string `json:"aid"`
		Net  string `json:"net"`
		Type string `json:"type"`
		Host string `json:"host"`
		Path string `json:"path"`
		TLS  string `json:"tls"`
		SNI  string `json:"sni"`
		Fp   string `json:"fp"`
	}{
		V: "2", Ps: "VM-HK1", Add: "198.51.100.7", Port: "443",
		ID: "22222222-2222-2222-2222-222222222222", Net: "ws", Type: "none",
		Host: "cdn.example.com", Path: "/ws", TLS: "tls", SNI: "cdn.example.com",
	}
	raw, _ := json.Marshal(vm)
	payload := "vmess://" + base64.StdEncoding.EncodeToString(raw)

	nodes := parse(t, payload)
	if len(nodes) != 1 {
		t.Fatalf("got %d nodes: %+v", len(nodes), nodes)
	}
	n := nodes[0]
	if n.Protocol != domain.ProtocolVmess || n.Server != "198.51.100.7" || n.Port != 443 {
		t.Fatalf("node = %+v", n)
	}
	if n.Transport != "ws" || !n.TLS || n.Path != "/ws" || n.Host != "cdn.example.com" {
		t.Fatalf("vmess transport = %+v", n)
	}
}

func TestParseShadowsocks(t *testing.T) {
	content := "ss://" + base64Std("aes-256-gcm:password1") + "@203.0.113.2:8388#SS-1"
	nodes := parse(t, content)
	if len(nodes) != 1 {
		t.Fatalf("got %d nodes", len(nodes))
	}
	n := nodes[0]
	if n.Protocol != domain.ProtocolShadowsocks || n.Server != "203.0.113.2" || n.Port != 8388 {
		t.Fatalf("node = %+v", n)
	}
	if n.Params["method"] != "aes-256-gcm" || n.Params["password"] != "password1" {
		t.Fatalf("ss params = %+v", n.Params)
	}
}

func TestParseHysteria2AndTuic(t *testing.T) {
	nodes := parse(t, `
hysteria2://hy2-pass@203.0.113.3:443?sni=example.com&obfs=salamander&obfs-password=xyz#HY2
tuic://33333333-3333-3333-3333-333333333333:tuicpass@203.0.113.4:443?sni=example.com&alpn=h3#TUIC
`)
	if len(nodes) != 2 {
		t.Fatalf("got %d nodes", len(nodes))
	}
	if nodes[0].Protocol != domain.ProtocolHysteria2 || nodes[0].Params["password"] != "hy2-pass" {
		t.Fatalf("hy2 = %+v", nodes[0])
	}
	if nodes[1].Protocol != domain.ProtocolTuic || nodes[1].Params["password"] != "tuicpass" {
		t.Fatalf("tuic = %+v", nodes[1])
	}
}

func TestParseTrojanAndSSR(t *testing.T) {
	ssr := "173.240.115.212:443:auth_sha1_v4:aes-256-gcm:tls1.2_ticket_auth:" + base64Std("ssrpass")
	content := "trojan://trojan-pass@203.0.113.5:443?sni=example.com#TR\nssr://" + base64.RawURLEncoding.EncodeToString([]byte(ssr))
	nodes := parse(t, content)
	if len(nodes) != 2 {
		t.Fatalf("got %d nodes", len(nodes))
	}
	if nodes[0].Protocol != domain.ProtocolTrojan || nodes[0].Params["password"] != "trojan-pass" {
		t.Fatalf("trojan = %+v", nodes[0])
	}
	if nodes[1].Protocol != domain.ProtocolSSR || nodes[1].Params["method"] != "aes-256-gcm" || nodes[1].Params["password"] != "ssrpass" {
		t.Fatalf("ssr = %+v", nodes[1])
	}
}

func TestParseBase64SubscriptionBlob(t *testing.T) {
	plain := "vless://11111111-1111-1111-1111-111111111111@203.0.113.1:443#A\nss://" + base64Std("aes-256-gcm:password1") + "@203.0.113.2:8388#B\n"
	nodes := parse(t, base64Std(plain))
	if len(nodes) != 2 {
		t.Fatalf("got %d nodes from base64 blob", len(nodes))
	}
}

func TestParseSkipsMalformedLines(t *testing.T) {
	content := "vless://11111111-1111-1111-1111-111111111111@203.0.113.1:443#OK\ngarbage-line\nvless://bad@host\n"
	nodes, errs := New().Parse([]byte(content), "sub-1", fixedClock{})
	if len(nodes) != 1 {
		t.Fatalf("got %d nodes, want 1", len(nodes))
	}
	if len(errs) != 2 {
		t.Fatalf("got %d errors, want 2", len(errs))
	}
}

func TestNodeDedupeHashNormalizes(t *testing.T) {
	a := domain.NodeDedupeHash("Example.COM", 443, domain.ProtocolVless, map[string]string{"uuid": "x"})
	b := domain.NodeDedupeHash("example.com", 443, domain.ProtocolVless, map[string]string{"uuid": "x"})
	if a != b {
		t.Fatal("hash must be case-insensitive for server")
	}
	if len(a) != 32 {
		t.Fatal("hash must be 128-bit hex")
	}
}
