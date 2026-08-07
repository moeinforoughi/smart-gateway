package singbox

import (
	"encoding/json"
	"strings"
	"testing"

	"smart-gateway/internal/domain"
)

func node(tag, protocol string, params map[string]string) domain.Node {
	n := domain.Node{
		ID:       tag,
		Protocol: domain.Protocol(protocol),
		Server:   "1.2.3.4",
		Port:     443,
		Params:   params,
	}
	if n.Params == nil {
		n.Params = map[string]string{}
	}
	return n
}

func findOutbound(cfg *Config, tag string) map[string]any {
	for _, ob := range cfg.Outbounds {
		if ob["tag"] == tag {
			return ob
		}
	}
	return nil
}

func TestGenerateSelectorFirstWithNoDrop(t *testing.T) {
	g := NewGenerator(GeneratorOptions{ClashAPIListen: "127.0.0.1:9090", ClashAPISecret: "s3cret", CacheFile: "data/cache.db"})
	cfg, err := g.Generate([]domain.Node{node("n1", "vless", map[string]string{"uuid": "u1"})})
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Outbounds) == 0 {
		t.Fatal("no outbounds")
	}
	sel := cfg.Outbounds[0]
	if sel["type"] != "selector" || sel["tag"] != "smart" {
		t.Fatalf("expected smart selector first, got %+v", sel)
	}
	if sel["interrupt_exist_connections"] != false {
		t.Fatalf("no-drop switch requires interrupt_exist_connections=false")
	}
	if cfg.Route["final"] != "smart" {
		t.Fatalf("route.final = %v, want smart", cfg.Route["final"])
	}
	exp, ok := cfg.Experimental["clash_api"].(map[string]any)
	if !ok {
		t.Fatalf("missing clash_api block: %+v", cfg.Experimental)
	}
	if exp["secret"] != "s3cret" {
		t.Fatalf("clash_api secret = %v", exp["secret"])
	}
	cf, ok := cfg.Experimental["cache_file"].(map[string]any)
	if !ok || cf["store_selected"] != true {
		t.Fatalf("missing cache_file.store_selected: %+v", cfg.Experimental["cache_file"])
	}
}

func TestGenerateSkipsSSRAndUnknown(t *testing.T) {
	g := NewGenerator(GeneratorOptions{})
	nodes := []domain.Node{
		node("n1", "vless", map[string]string{"uuid": "u1"}),
		node("n2", "ssr", map[string]string{}),
		node("n3", "wireguard", map[string]string{}),
	}
	cfg, err := g.Generate(nodes)
	if err != nil {
		t.Fatal(err)
	}
	if findOutbound(cfg, "n2") != nil || findOutbound(cfg, "n3") != nil {
		t.Fatal("SSR/unknown protocols must be skipped")
	}
	if findOutbound(cfg, "n1") == nil {
		t.Fatal("vless node missing")
	}
}

func TestGenerateNoUsableOutbounds(t *testing.T) {
	g := NewGenerator(GeneratorOptions{})
	if _, err := g.Generate([]domain.Node{node("n1", "ssr", map[string]string{})}); err == nil {
		t.Fatal("expected error when nothing is usable")
	}
}

func TestVlessRealityOutbound(t *testing.T) {
	g := NewGenerator(GeneratorOptions{})
	n := node("r1", "vless", map[string]string{
		"uuid":     "u1",
		"security": "reality",
		"pbk":      "pubkey",
		"sid":      "abcd",
	})
	n.SNI = "example.com"
	n.Fingerprint = "chrome"
	n.Flow = "xtls-rprx-vision"
	n.TLS = true
	cfg, err := g.Generate([]domain.Node{n})
	if err != nil {
		t.Fatal(err)
	}
	ob := findOutbound(cfg, "r1")
	if ob["uuid"] != "u1" || ob["flow"] != "xtls-rprx-vision" {
		t.Fatalf("vless fields wrong: %+v", ob)
	}
	tls := ob["tls"].(map[string]any)
	real := tls["reality"].(map[string]any)
	if tls["server_name"] != "example.com" || real["public_key"] != "pubkey" || real["short_id"] != "abcd" {
		t.Fatalf("reality tls wrong: %+v", tls)
	}
	if _, ok := tls["utls"]; !ok {
		t.Fatalf("missing utls fingerprint block: %+v", tls)
	}
}

func TestVmessAlterID(t *testing.T) {
	g := NewGenerator(GeneratorOptions{})
	n := node("m1", "vmess", map[string]string{"uuid": "u1", "alterId": "2"})
	n.Transport = "ws"
	n.Path = "/v2"
	n.Host = "cdn.example.com"
	cfg, err := g.Generate([]domain.Node{n})
	if err != nil {
		t.Fatal(err)
	}
	ob := findOutbound(cfg, "m1")
	if ob["alter_id"].(int) != 2 || ob["security"] != "auto" {
		t.Fatalf("vmess fields wrong: %+v", ob)
	}
	tr := ob["transport"].(map[string]any)
	if tr["type"] != "ws" || tr["path"] != "/v2" {
		t.Fatalf("ws transport wrong: %+v", tr)
	}
	if tr["headers"].(map[string]any)["Host"] != "cdn.example.com" {
		t.Fatalf("ws host header missing")
	}
}

func TestTrojanAndShadowsocks(t *testing.T) {
	g := NewGenerator(GeneratorOptions{})
	trojan := node("t1", "trojan", map[string]string{"password": "pw"})
	ss := node("s1", "shadowsocks", map[string]string{"method": "aes-128-gcm", "password": "pw"})
	cfg, err := g.Generate([]domain.Node{trojan, ss})
	if err != nil {
		t.Fatal(err)
	}
	if ob := findOutbound(cfg, "t1"); ob["password"] != "pw" {
		t.Fatalf("trojan: %+v", ob)
	}
	if ob := findOutbound(cfg, "s1"); ob["method"] != "aes-128-gcm" {
		t.Fatalf("ss: %+v", ob)
	}
}

func TestJSONRenders(t *testing.T) {
	g := NewGenerator(GeneratorOptions{LogLevel: "debug"})
	cfg, err := g.Generate([]domain.Node{node("n1", "vless", map[string]string{"uuid": "u1"})})
	if err != nil {
		t.Fatal(err)
	}
	data, err := cfg.JSON()
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("generated config is not valid JSON: %v", err)
	}
	if !strings.Contains(string(data), `"smart"`) {
		t.Fatalf("selector tag missing from JSON")
	}
}

func TestCustomSelectorTag(t *testing.T) {
	g := NewGenerator(GeneratorOptions{SelectorTag: "outbound"})
	cfg, err := g.Generate([]domain.Node{node("n1", "vless", map[string]string{"uuid": "u1"})})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Outbounds[0]["tag"] != "outbound" || cfg.Route["final"] != "outbound" {
		t.Fatalf("custom tag not applied: %+v", cfg.Outbounds[0])
	}
}
