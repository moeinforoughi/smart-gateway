package singbox

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func newTestServer(t *testing.T, handler http.HandlerFunc) (*ClashClient, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return NewClashClient(srv.URL, "sekret", time.Second), srv
}

func TestApplySelection(t *testing.T) {
	var gotMethod, gotBody, gotAuth string
	client, _ := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotAuth = r.Header.Get("Authorization")
		buf := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(buf)
		gotBody = string(buf)
		w.WriteHeader(http.StatusNoContent)
	})
	if err := client.ApplySelection(context.Background(), "smart", "node-1"); err != nil {
		t.Fatalf("ApplySelection: %v", err)
	}
	if gotMethod != http.MethodPut || gotAuth != "Bearer sekret" {
		t.Fatalf("method=%s auth=%s", gotMethod, gotAuth)
	}
	var payload map[string]string
	if err := json.Unmarshal([]byte(gotBody), &payload); err != nil || payload["name"] != "node-1" {
		t.Fatalf("body=%s", gotBody)
	}
}

func TestApplySelectionHTTPError(t *testing.T) {
	client, _ := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	if err := client.ApplySelection(context.Background(), "smart", "nope"); err == nil {
		t.Fatal("expected error on 404")
	}
}

func TestCurrent(t *testing.T) {
	client, _ := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/proxies/smart" {
			t.Errorf("path=%s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"now":"node-2"}`))
	})
	now, err := client.Current(context.Background(), "smart")
	if err != nil {
		t.Fatal(err)
	}
	if now != "node-2" {
		t.Fatalf("now=%s", now)
	}
}

func TestDelay(t *testing.T) {
	client, _ := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/proxies/node-9/delay") {
			t.Errorf("path=%s", r.URL.Path)
		}
		if r.URL.Query().Get("timeout") == "" || r.URL.Query().Get("url") == "" {
			t.Errorf("missing query params: %s", r.URL.RawQuery)
		}
		_, _ = w.Write([]byte(`{"delay":123}`))
	})
	d, err := client.Delay(context.Background(), "node-9", 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if d != 123*time.Millisecond {
		t.Fatalf("delay=%v", d)
	}
}

func TestConnections(t *testing.T) {
	client, _ := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{
			"downloadTotal":1000,"uploadTotal":500,
			"connections":[{"id":"c1","chains":["smart","n1"],"metadata":{"host":"x.com"}}]
		}`))
	})
	snap, err := client.Connections(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snap.DownloadTotal != 1000 || snap.UploadTotal != 500 || len(snap.Connections) != 1 {
		t.Fatalf("snapshot=%+v", snap)
	}
}

func TestTrafficSSE(t *testing.T) {
	client, _ := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		f := w.(http.Flusher)
		fmt.Fprint(w, "data: {\"up\":10,\"down\":20}\n\n")
		f.Flush()
		fmt.Fprint(w, "data: {\"up\":100,\"down\":200}\n\n")
		f.Flush()
	})
	up, down, err := client.Traffic(context.Background(), 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if up != 100 || down != 200 {
		t.Fatalf("up=%d down=%d", up, down)
	}
}

func TestTrafficNon200(t *testing.T) {
	client, _ := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	})
	if _, _, err := client.Traffic(context.Background(), 100*time.Millisecond); err == nil {
		t.Fatal("expected error on 403")
	}
}
