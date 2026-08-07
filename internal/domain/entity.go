package domain

import "time"

// Protocol identifies the wire protocol of a node (FR-200).
type Protocol string

const (
	ProtocolVless       Protocol = "vless"
	ProtocolVmess       Protocol = "vmess"
	ProtocolTrojan      Protocol = "trojan"
	ProtocolShadowsocks Protocol = "shadowsocks"
	ProtocolSSR         Protocol = "ssr"
	ProtocolHysteria2   Protocol = "hysteria2"
	ProtocolTuic        Protocol = "tuic"
	ProtocolSOCKS       Protocol = "socks"
	ProtocolHTTP        Protocol = "http"
)

// Valid reports whether p is a supported protocol.
func (p Protocol) Valid() bool {
	switch p {
	case ProtocolVless, ProtocolVmess, ProtocolTrojan, ProtocolShadowsocks,
		ProtocolSSR, ProtocolHysteria2, ProtocolTuic, ProtocolSOCKS, ProtocolHTTP:
		return true
	}
	return false
}

// Node is a single usable proxy endpoint discovered from a subscription.
// Identity: ID is the stable UUIDv7 stored in the database (FR-206);
// DedupeHash is the content identity used to merge duplicates (FR-204/205).
type Node struct {
	ID             string // UUIDv7, stable across refreshes (FR-206)
	DedupeHash     string // sha256(server|port|protocol|params)
	SubscriptionID string // owning subscription UUIDv7
	Protocol       Protocol
	Server         string
	Port           int
	Name           string // display name from the share link
	Transport      string // tcp | ws | grpc | http | ws+tls | ...
	TLS            bool
	SNI            string            // server name for TLS verification
	Fingerprint    string            // sing-box uTLS fingerprint
	Host           string            // http Host header (ws/http transports)
	Path           string            // ws/http/gRPC path
	ServiceName    string            // gRPC service name
	Flow           string            // vless flow (e.g. xtls-rprx-vision)
	Params         map[string]string // protocol-specific opaque params (uuid, password, method, ...)
	Raw            string            // original share link, retained for debugging
	Tags           []string

	State          NodeState // health lifecycle (architecture §16)
	StateChangedAt time.Time

	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt *time.Time // soft delete marker (FR-301)
}

// SubscriptionState models the lifecycle of a subscription.
type SubscriptionState string

const (
	SubscriptionActive   SubscriptionState = "active"
	SubscriptionDisabled SubscriptionState = "disabled"
	SubscriptionError    SubscriptionState = "error"
)

// Subscription is a provider source of nodes (FR-100 series).
// ETag/LastModified cache the HTTP response so unchanged sources are not
// re-downloaded (FR-109).
type Subscription struct {
	ID                  string // UUIDv7
	URL                 string
	Name                string
	Provider            string
	Priority            int // higher wins when sources disagree
	Tags                []string
	Enabled             bool
	Headers             map[string]string // extra HTTP headers
	UpdateEvery         time.Duration     // per-source override (FR-112)
	Timeout             time.Duration
	ETag                string
	LastModified        string
	State               SubscriptionState
	ConsecutiveFailures int
	LastFetchAt         time.Time
	NextFetchAt         time.Time
	NodeCount           int
	LastError           string
	CreatedAt           time.Time
	UpdatedAt           time.Time
}
