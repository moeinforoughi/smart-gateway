// Package api implements the versioned REST API and serves the embedded
// dashboard (M5). It is the outermost internal layer (architecture rank 4):
// it reads from repository/domain ports and applies changes through narrow
// interfaces wired by the composition root (internal/app). Monitoring reads
// are public (FR-1101); every state-changing operation requires the admin
// Bearer token (FR-1102, NFR-143).
package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"smart-gateway/internal/config"
	"smart-gateway/internal/dashboard"
	"smart-gateway/internal/domain"
	"smart-gateway/internal/infrastructure/singbox"
	"smart-gateway/internal/log"
	"smart-gateway/internal/subscription"
)

// Store is the persistence surface the API needs. The SQLite repository
// implements it; tests substitute an in-memory fake.
type Store interface {
	ListAllNodes(ctx context.Context) ([]domain.Node, error)
	GetNode(ctx context.Context, id string) (*domain.Node, error)
	CountNodes(ctx context.Context) (int, error)
	CountNodesByState(ctx context.Context) (map[domain.NodeState]int, error)
	GetNodeHealth(ctx context.Context, nodeID string) (domain.NodeHealth, error)
	GetNodeScore(ctx context.Context, nodeID string) (domain.NodeScore, error)
	ScoreHistory(ctx context.Context, nodeID string, limit int) ([]domain.NodeScore, error)
	TopEligible(ctx context.Context, limit int) ([]domain.NodeScore, error)
	ListScores(ctx context.Context) ([]domain.NodeScore, error)
	CountScores(ctx context.Context) (int, error)
	ListSubscriptions(ctx context.Context) ([]domain.Subscription, error)
	GetSubscription(ctx context.Context, id string) (*domain.Subscription, error)
	SaveSubscription(ctx context.Context, s *domain.Subscription) error
	DeleteSubscription(ctx context.Context, id string) (bool, error)
	ListSwitchRecords(ctx context.Context, limit int) ([]domain.SwitchRecord, error)
}

// RouteManager is the outbound control port. The routing.Manager implements
// it; tests substitute a stub.
type RouteManager interface {
	Current() (nodeID, reason string)
	Override(ctx context.Context, nodes []domain.Node, nodeID, reason string) error
	Release(ctx context.Context, nodes []domain.Node) error
	EnsureActive(ctx context.Context, nodes []domain.Node) error
	Evaluate(ctx context.Context, nodes []domain.Node) (domain.SwitchDecision, error)
}

// Subscriber triggers an immediate subscription refresh.
type Subscriber interface {
	RefreshByID(ctx context.Context, id string) (*subscription.Result, error)
}

// Core reports data-plane status.
type Core interface {
	Running() bool
	Failed() bool
	RestartsInLastHour() int
}

// ReloadFunc performs a config-only reload and reports what changed.
type ReloadFunc func(ctx context.Context) (ReloadResult, error)

// ReloadResult describes what a config reload touched.
type ReloadResult struct {
	Reloaded        []string `json:"reloaded"`
	RequiresRestart []string `json:"requires_restart"`
}

// ClientConfigFunc builds the stable client configs for the configured
// inbounds (public endpoint, M5).
type ClientConfigFunc func() (*singbox.ClientConfig, error)

// Deps wires every dependency into the API server.
type Deps struct {
	Config       *config.Config
	Store        Store
	Routes       RouteManager
	Subscriber   Subscriber
	Core         Core
	ClientConfig ClientConfigFunc
	Reload       ReloadFunc
	Version      string
	Logger       *log.Logger
}

// Server is the API + dashboard HTTP server.
type Server struct {
	deps    Deps
	started time.Time

	token   string
	logger  *log.Logger
	limiter *rateLimiter

	mu   sync.Mutex
	auto map[string]time.Time // nodeID -> auto-release time
}

// New builds the API server with its route table registered.
func New(deps Deps) *Server {
	if deps.Logger == nil {
		deps.Logger = log.New(nil, 0)
	}
	return &Server{
		deps:    deps,
		started: time.Now(),
		token:   deps.Config.API.Token,
		logger:  deps.Logger,
		limiter: newRateLimiter(120, time.Minute),
		auto:    make(map[string]time.Time),
	}
}

// Token returns the admin token enforced by the server.
func (s *Server) Token() string { return s.token }

// Handler returns the fully wrapped HTTP handler (recovery, correlation,
// request logging, then the routed mux).
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	s.register(mux)
	return s.recover(s.correlation(mux))
}

func (s *Server) register(mux *http.ServeMux) {
	// Dashboard UI (public; admin actions inside require the token).
	if s.deps.Config.API.Dashboard {
		mux.Handle("/dashboard", dashboard.Handler())
		mux.Handle("/dashboard/", dashboard.Handler())
	}

	// Health & liveness.
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)

	// Status & metrics (public).
	mux.HandleFunc("GET /api/v1/status", s.handleStatus)
	mux.HandleFunc("GET /api/v1/metrics", s.handleMetrics)

	// Nodes (public).
	mux.HandleFunc("GET /api/v1/nodes", s.handleListNodes)
	mux.HandleFunc("GET /api/v1/nodes/{id}", s.handleGetNode)

	// Outbound control.
	mux.HandleFunc("GET /api/v1/outbound", s.handleOutbound)
	mux.HandleFunc("POST /api/v1/outbound/override", s.admin(s.limit(s.handleOverride)))
	mux.HandleFunc("DELETE /api/v1/outbound/override", s.admin(s.limit(s.handleRelease)))
	mux.HandleFunc("GET /api/v1/outbound/evaluate", s.admin(s.limit(s.handleEvaluate)))

	// Subscriptions.
	mux.HandleFunc("GET /api/v1/subscriptions", s.handleListSubscriptions)
	mux.HandleFunc("POST /api/v1/subscriptions", s.admin(s.limit(s.handleAddSubscription)))
	mux.HandleFunc("DELETE /api/v1/subscriptions/{id}", s.admin(s.limit(s.handleDeleteSubscription)))
	mux.HandleFunc("POST /api/v1/subscriptions/{id}/refresh", s.admin(s.limit(s.handleRefreshSubscription)))

	// Operations.
	mux.HandleFunc("GET /api/v1/history", s.handleHistory)
	mux.HandleFunc("GET /api/v1/client-config", s.handleClientConfig)
	mux.HandleFunc("POST /api/v1/reload", s.admin(s.limit(s.handleReload)))
}

// ---- middleware ----

func (s *Server) recover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				s.logger.Error("panic recovered", "path", r.URL.Path, "panic", rec)
				writeErr(w, http.StatusInternalServerError, "internal", "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func (s *Server) correlation(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" {
			id = domain.NewUUIDv7(time.Now())
		}
		w.Header().Set("X-Request-ID", id)
		start := time.Now()
		lw := &logWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(lw, r)
		s.logger.Info("http request", "method", r.Method, "path", r.URL.Path,
			"status", lw.status, "duration_ms", time.Since(start).Milliseconds(), "request_id", id)
	})
}

// admin rejects requests without a valid Bearer token (FR-1102).
func (s *Server) admin(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.authorized(r) {
			writeErr(w, http.StatusUnauthorized, "unauthorized", "admin token required")
			return
		}
		next(w, r)
	}
}

func (s *Server) authorized(r *http.Request) bool {
	const prefix = "Bearer "
	auth := r.Header.Get("Authorization")
	if len(auth) <= len(prefix) || !strings.EqualFold(auth[:len(prefix)], prefix) {
		return false
	}
	return s.constantEquals(auth[len(prefix):], s.token)
}

func (s *Server) constantEquals(a, b string) bool {
	if a == "" || b == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

// limit applies the fixed-window per-IP rate limit to admin endpoints.
func (s *Server) limit(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if ip := clientIP(r); !s.limiter.Allow(ip) {
			writeErr(w, http.StatusTooManyRequests, "rate_limited", "rate limit exceeded")
			return
		}
		next(w, r)
	}
}

func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if i := strings.IndexByte(xff, ','); i >= 0 {
			return strings.TrimSpace(xff[:i])
		}
		return strings.TrimSpace(xff)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// ---- helpers ----

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]string{"code": code, "message": msg},
	})
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ready"})
}

// logWriter captures the response status for request logging.
type logWriter struct {
	http.ResponseWriter
	status int
}

func (l *logWriter) WriteHeader(code int) {
	l.status = code
	l.ResponseWriter.WriteHeader(code)
}
