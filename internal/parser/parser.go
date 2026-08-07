// Package parser turns subscription payloads into domain nodes (FR-200).
// It understands the common share-link dialects: vless, vmess, trojan,
// shadowsocks, ssr, hysteria2, tuic, socks and http(s). Malformed lines are
// skipped and reported so callers can log a summary (FR-205).
package parser

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"

	"smart-gateway/internal/domain"
)

// Parser is stateless; a single instance may be shared.
type Parser struct{}

// New returns a Parser.
func New() *Parser { return &Parser{} }

// Parse decodes content (base64 when needed) and converts every recognised
// share link into a domain.Node. The returned error slice carries one entry
// per skipped line (nil when nothing was skipped).
func (p *Parser) Parse(content []byte, subID string, now domain.Clock) ([]domain.Node, []error) {
	text := decodeBody(content)
	nodes := make([]domain.Node, 0, 64)
	var errs []error
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		n, err := p.parseLine(line)
		if err != nil {
			errs = append(errs, fmt.Errorf("line %q: %w", line, err))
			continue
		}
		n.SubscriptionID = subID
		n.DedupeHash = domain.NodeDedupeHash(n.Server, n.Port, n.Protocol, n.Params)
		n.State = domain.StateUnknown
		n.CreatedAt = now.Now()
		n.UpdatedAt = now.Now()
		nodes = append(nodes, *n)
	}
	return nodes, errs
}

// decodeBody returns the payload text, transparently unwrapping the single
// base64 blob most providers emit. Plain text with share links is returned
// untouched.
func decodeBody(content []byte) string {
	if bytesHaveScheme(content) {
		return string(content)
	}
	trimmed := strings.TrimSpace(string(content))
	for _, enc := range []*base64.Encoding{
		base64.StdEncoding, base64.RawStdEncoding,
		base64.RawURLEncoding, base64.URLEncoding,
	} {
		dec, err := enc.DecodeString(strings.Join(strings.Fields(trimmed), ""))
		if err == nil && bytesHaveScheme(dec) {
			return string(dec)
		}
	}
	return string(content)
}

func bytesHaveScheme(b []byte) bool {
	s := string(b)
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		idx := strings.Index(line, "://")
		if idx <= 0 {
			continue
		}
		scheme := strings.ToLower(line[:idx])
		switch scheme {
		case "vless", "vmess", "trojan", "ss", "ssr", "hysteria2", "hy2", "tuic", "socks", "socks5", "http", "https":
			return true
		}
	}
	return false
}

func (p *Parser) parseLine(line string) (*domain.Node, error) {
	idx := strings.Index(line, "://")
	if idx <= 0 {
		return nil, errors.New("not a share link")
	}
	scheme := strings.ToLower(line[:idx])
	rest := line[idx+3:]
	switch scheme {
	case "vless":
		return parseVless(rest)
	case "vmess":
		return parseVmess(rest)
	case "trojan":
		return parseTrojan(rest)
	case "ss":
		return parseShadowsocks(rest)
	case "ssr":
		return parseSSR(rest)
	case "hysteria2", "hy2":
		return parseHysteria2(rest)
	case "tuic":
		return parseTuic(rest)
	case "socks", "socks5":
		return parseSocks(rest)
	case "http", "https":
		return parseHTTP(rest, scheme == "https")
	default:
		return nil, fmt.Errorf("unsupported scheme %q", scheme)
	}
}

func splitHostPort(hostport string) (string, int, error) {
	host, portStr, err := net.SplitHostPort(hostport)
	if err != nil {
		return "", 0, fmt.Errorf("invalid host:port: %w", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
		return "", 0, fmt.Errorf("invalid port %q", portStr)
	}
	return host, port, nil
}

func nameFrom(fragment string, fallback string) string {
	if fragment == "" {
		return fallback
	}
	if n, err := url.PathUnescape(fragment); err == nil && n != "" {
		return n
	}
	return fragment
}

func baseNode(protocol domain.Protocol, server string, port int, fragment string, params map[string]string) *domain.Node {
	name := fragment
	if name == "" {
		name = fmt.Sprintf("%s-%s:%d", protocol, server, port)
	}
	return &domain.Node{
		Protocol: protocol,
		Server:   server,
		Port:     port,
		Name:     name,
		Params:   params,
	}
}

func parseVless(rest string) (*domain.Node, error) {
	u, err := url.Parse("vless://" + rest)
	if err != nil || u.Host == "" {
		return nil, fmt.Errorf("invalid vless link: %w", err)
	}
	server, port, err := splitHostPort(u.Host)
	if err != nil {
		return nil, err
	}
	q := u.Query()
	params := map[string]string{
		"uuid":        u.User.Username(),
		"encryption":  q.Get("encryption"),
		"flow":        q.Get("flow"),
		"security":    q.Get("security"),
		"type":        q.Get("type"),
		"host":        q.Get("host"),
		"path":        q.Get("path"),
		"serviceName": q.Get("serviceName"),
		"headerType":  q.Get("headerType"),
		"sni":         q.Get("sni"),
		"fp":          q.Get("fp"),
		"sid":         q.Get("sid"),
		"pbk":         q.Get("pbk"),
	}
	node := baseNode(domain.ProtocolVless, server, port, u.Fragment, params)
	node.Transport = orDefault(q.Get("type"), "tcp")
	node.Flow = q.Get("flow")
	node.Host = q.Get("host")
	node.Path = q.Get("path")
	node.ServiceName = q.Get("serviceName")
	sec := q.Get("security")
	node.TLS = sec == "tls" || sec == "reality"
	if sec == "reality" {
		node.Fingerprint = q.Get("fp")
	}
	if q.Get("sni") != "" {
		node.SNI = q.Get("sni")
	} else if sec == "tls" && q.Get("host") != "" {
		node.SNI = q.Get("host")
	}
	return node, nil
}

type vmessLink struct {
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
	Alpn string `json:"alpn"`
	Fp   string `json:"fp"`
}

func parseVmess(rest string) (*domain.Node, error) {
	payload := rest
	if i := strings.IndexAny(payload, "#? "); i > 0 {
		payload = payload[:i]
	}
	raw, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		if raw, err = base64.RawStdEncoding.DecodeString(payload); err != nil {
			return nil, fmt.Errorf("invalid vmess payload: %w", err)
		}
	}
	var v vmessLink
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, fmt.Errorf("invalid vmess json: %w", err)
	}
	port, err := strconv.Atoi(v.Port)
	if err != nil || port < 1 || port > 65535 {
		return nil, fmt.Errorf("invalid vmess port %q", v.Port)
	}
	params := map[string]string{
		"uuid":    v.ID,
		"alterId": v.Aid,
		"type":    v.Net,
		"host":    v.Host,
		"path":    v.Path,
		"tls":     v.TLS,
		"sni":     v.SNI,
		"alpn":    v.Alpn,
		"fp":      v.Fp,
	}
	node := baseNode(domain.ProtocolVmess, v.Add, port, v.Ps, params)
	node.Transport = orDefault(v.Net, "tcp")
	node.Host = v.Host
	node.Path = v.Path
	node.TLS = strings.EqualFold(v.TLS, "tls")
	if v.SNI != "" {
		node.SNI = v.SNI
	} else if node.TLS {
		node.SNI = v.Add
	}
	return node, nil
}

func parseTrojan(rest string) (*domain.Node, error) {
	u, err := url.Parse("trojan://" + rest)
	if err != nil || u.Host == "" {
		return nil, fmt.Errorf("invalid trojan link: %w", err)
	}
	server, port, err := splitHostPort(u.Host)
	if err != nil {
		return nil, err
	}
	q := u.Query()
	params := map[string]string{
		"password":      u.User.Username(),
		"sni":           q.Get("sni"),
		"type":          q.Get("type"),
		"host":          q.Get("host"),
		"path":          q.Get("path"),
		"serviceName":   q.Get("serviceName"),
		"alpn":          q.Get("alpn"),
		"fp":            q.Get("fp"),
		"allowInsecure": q.Get("allowInsecure"),
	}
	node := baseNode(domain.ProtocolTrojan, server, port, u.Fragment, params)
	node.Transport = orDefault(q.Get("type"), "tcp")
	node.Host = q.Get("host")
	node.Path = q.Get("path")
	node.ServiceName = q.Get("serviceName")
	node.TLS = true
	node.SNI = orDefault(q.Get("sni"), server)
	return node, nil
}

func parseShadowsocks(rest string) (*domain.Node, error) {
	// Two dialects:
	//   ss://BASE64(method:password)@host:port?plugin=...#name
	//   ss://method:password@host:port#name
	u, err := url.Parse("ss://" + rest)
	if err != nil || u.Host == "" {
		return nil, fmt.Errorf("invalid shadowsocks link: %w", err)
	}
	server, port, err := splitHostPort(u.Host)
	if err != nil {
		return nil, err
	}
	method := u.User.Username()
	password, _ := u.User.Password()
	if password == "" {
		// Dialect where the whole userinfo is BASE64(method:password).
		dec, derr := base64.StdEncoding.DecodeString(method)
		if derr != nil {
			dec, derr = base64.RawStdEncoding.DecodeString(method)
		}
		if derr == nil {
			sep := strings.Index(string(dec), ":")
			if sep > 0 {
				method = string(dec[:sep])
				password = string(dec[sep+1:])
			}
		}
	}
	if method == "" {
		return nil, errors.New("missing cipher")
	}
	q := u.Query()
	params := map[string]string{
		"method":   method,
		"password": password,
		"plugin":   q.Get("plugin"),
	}
	node := baseNode(domain.ProtocolShadowsocks, server, port, u.Fragment, params)
	node.Transport = "tcp"
	return node, nil
}

// parseSSR handles the SSR share-link format. The main body is
// base64url(server:port:protocol:method:obfs:base64url(password)); extra
// settings arrive as query parameters, also base64url encoded.
// decodeSSRBase64 decodes the SSR custom base64, tolerating padding variants.
func decodeSSRBase64(s string) ([]byte, error) {
	var lastErr error
	for _, enc := range []*base64.Encoding{
		base64.RawURLEncoding, base64.RawStdEncoding,
		base64.URLEncoding, base64.StdEncoding,
	} {
		dec, err := enc.DecodeString(s)
		if err == nil {
			return dec, nil
		}
		lastErr = err
	}
	// Some providers emit standard base64 without padding; strip and retry.
	if dec, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(s, "=")); err == nil {
		return dec, nil
	} else {
		lastErr = err
	}
	return nil, lastErr
}

func parseSSR(rest string) (*domain.Node, error) {
	body, query := rest, ""
	if i := strings.IndexByte(rest, '?'); i >= 0 {
		body, query = rest[:i], rest[i+1:]
	}
	raw, err := decodeSSRBase64(body)
	if err != nil {
		return nil, fmt.Errorf("invalid ssr payload: %w", err)
	}
	parts := strings.Split(string(raw), ":")
	if len(parts) != 6 {
		return nil, fmt.Errorf("ssr fields = %d, want 6", len(parts))
	}
	server := parts[0]
	port, err := strconv.Atoi(parts[1])
	if err != nil || port < 1 || port > 65535 {
		return nil, fmt.Errorf("invalid ssr port %q", parts[1])
	}
	proto, method, obfs := parts[2], parts[3], parts[4]
	passBytes, err := decodeSSRBase64(parts[5])
	if err != nil {
		return nil, fmt.Errorf("invalid ssr password: %w", err)
	}
	params := map[string]string{
		"protocol": proto,
		"method":   method,
		"obfs":     obfs,
		"password": string(passBytes),
	}
	// Query parameters (remarks, group, obfsparam, protoparam) are base64url.
	for _, kv := range strings.Split(query, "&") {
		eq := strings.IndexByte(kv, '=')
		if eq <= 0 {
			continue
		}
		key, val := kv[:eq], kv[eq+1:]
		if dec, derr := base64.RawURLEncoding.DecodeString(val); derr == nil {
			val = string(dec)
		}
		switch key {
		case "remarks":
			params["name"] = val
		case "group":
			params["group"] = val
		case "obfsparam":
			params["obfsparam"] = val
		case "protoparam":
			params["protoparam"] = val
		}
	}
	node := baseNode(domain.ProtocolSSR, server, port, params["name"], params)
	node.Transport = "tcp"
	return node, nil
}

func parseHysteria2(rest string) (*domain.Node, error) {
	u, err := url.Parse("hysteria2://" + rest)
	if err != nil || u.Host == "" {
		return nil, fmt.Errorf("invalid hysteria2 link: %w", err)
	}
	server, port, err := splitHostPort(u.Host)
	if err != nil {
		return nil, err
	}
	q := u.Query()
	params := map[string]string{
		"password":      u.User.Username(),
		"sni":           q.Get("sni"),
		"insecure":      q.Get("insecure"),
		"obfs":          q.Get("obfs"),
		"obfs-password": q.Get("obfs-password"),
		"pinSHA256":     q.Get("pinSHA256"),
		"alpn":          q.Get("alpn"),
	}
	node := baseNode(domain.ProtocolHysteria2, server, port, u.Fragment, params)
	node.Transport = "quic"
	node.TLS = true
	node.SNI = orDefault(q.Get("sni"), server)
	return node, nil
}

func parseTuic(rest string) (*domain.Node, error) {
	u, err := url.Parse("tuic://" + rest)
	if err != nil || u.Host == "" {
		return nil, fmt.Errorf("invalid tuic link: %w", err)
	}
	server, port, err := splitHostPort(u.Host)
	if err != nil {
		return nil, err
	}
	q := u.Query()
	pass, _ := u.User.Password()
	params := map[string]string{
		"uuid":               u.User.Username(),
		"password":           pass,
		"sni":                q.Get("sni"),
		"alpn":               q.Get("alpn"),
		"congestion_control": q.Get("congestion_control"),
	}
	node := baseNode(domain.ProtocolTuic, server, port, u.Fragment, params)
	node.Transport = "quic"
	node.TLS = true
	node.SNI = orDefault(q.Get("sni"), server)
	return node, nil
}

func parseSocks(rest string) (*domain.Node, error) {
	u, err := url.Parse("socks://" + rest)
	if err != nil || u.Host == "" {
		return nil, fmt.Errorf("invalid socks link: %w", err)
	}
	server, port, err := splitHostPort(u.Host)
	if err != nil {
		return nil, err
	}
	user, pass := u.User.Username(), ""
	if u.User != nil {
		pass, _ = u.User.Password()
	}
	node := baseNode(domain.ProtocolSOCKS, server, port, u.Fragment, map[string]string{
		"user": user, "pass": pass,
	})
	node.Transport = "tcp"
	return node, nil
}

func parseHTTP(rest string, tls bool) (*domain.Node, error) {
	scheme := "http://"
	if tls {
		scheme = "https://"
	}
	u, err := url.Parse(scheme + rest)
	if err != nil || u.Host == "" {
		return nil, fmt.Errorf("invalid http link: %w", err)
	}
	server, port, err := splitHostPort(u.Host)
	if err != nil {
		return nil, err
	}
	user, pass := "", ""
	if u.User != nil {
		user = u.User.Username()
		pass, _ = u.User.Password()
	}
	node := baseNode(domain.ProtocolHTTP, server, port, u.Fragment, map[string]string{
		"user": user, "pass": pass, "tls": strconv.FormatBool(tls),
	})
	node.TLS = tls
	node.Transport = "http"
	return node, nil
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
