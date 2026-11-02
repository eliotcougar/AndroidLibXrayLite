package libv2ray

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	core "github.com/xtls/xray-core/core"
	coreserial "github.com/xtls/xray-core/infra/conf/serial"
)

func TestLegacyOutboundDelaySupportsGetOnlyEndpoints(t *testing.T) {
	var getRequests, headRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			headRequests.Add(1)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		getRequests.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	config := `{"log":{"loglevel":"none"},"outbounds":[{"protocol":"freedom","tag":"proxy"}]}`
	delay, err := MeasureOutboundDelay(config, server.URL)
	if err != nil || delay < 0 {
		t.Fatalf("working GET endpoint rejected: delay=%d error=%v", delay, err)
	}
	if getRequests.Load() != 2 || headRequests.Load() != 0 {
		t.Fatalf("legacy request policy changed: GET=%d HEAD=%d", getRequests.Load(), headRequests.Load())
	}
}

func TestProbeFallbackRetainsSingleHeadRequest(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Method != http.MethodHead {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	controller := NewProbeController()
	defer controller.Cancel()
	config := `{"log":{"loglevel":"none"},"outbounds":[{"protocol":"freedom","tag":"proxy"}]}`
	delay, err := controller.MeasureDelay(config, server.URL)
	if err != nil || delay < 0 {
		t.Fatalf("probe fallback failed: delay=%d error=%v", delay, err)
	}
	if requests.Load() != 1 {
		t.Fatalf("probe fallback made %d requests, want 1", requests.Load())
	}
}

func TestApplyRequestHeaders(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "https://example.com", nil)
	if err != nil {
		t.Fatal(err)
	}

	applyRequestHeaders(req, map[string]string{
		"host":   "origin.example",
		"X-Test": "value",
	})

	if req.Host != "origin.example" {
		t.Fatalf("Host = %q, want %q", req.Host, "origin.example")
	}
	if got := req.Header.Get("Host"); got != "" {
		t.Fatalf("Header[Host] = %q, want empty", got)
	}
	if got := req.Header.Get("X-Test"); got != "value" {
		t.Fatalf("X-Test = %q, want %q", got, "value")
	}
}

func TestDownloadUrlToFileWithHeaders(t *testing.T) {
	controller := testHTTPController(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirect" {
			w.Header().Set("Profile-Title", "intermediate")
			http.Redirect(w, r, "/subscription", http.StatusFound)
			return
		}
		if r.Header.Get("X-Test") != "request" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		w.Header().Set("Subscription-Userinfo", "upload=1; download=2; total=100; expire=2000000000")
		w.Header().Set("Profile-Title", "base64:dGVzdA==")
		w.Header().Set("Support-Url", "https://example.com/support")
		w.Header().Set("Profile-Web-Page-Url", "https://example.com/account")
		w.Header().Add("X-Repeated", "first")
		w.Header().Add("X-Repeated", "second")
		_, _ = w.Write([]byte("subscription body"))
	}))
	defer server.Close()
	path := filepath.Join(t.TempDir(), "subscription")
	headersJSON, err := controller.DownloadUrlToFileWithHeaders(server.URL+"/redirect", "proxy", `{"X-Test":"request"}`, path, 1000)
	if err != nil {
		t.Fatal(err)
	}
	var headers http.Header
	if err := json.Unmarshal([]byte(headersJSON), &headers); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{
		"Subscription-Userinfo": "upload=1; download=2; total=100; expire=2000000000",
		"Profile-Title":         "base64:dGVzdA==",
		"Support-Url":           "https://example.com/support",
		"Profile-Web-Page-Url":  "https://example.com/account",
	} {
		if got := headers.Get(name); got != want {
			t.Errorf("%s = %q, want %q", name, got, want)
		}
	}
	if got := headers.Values("X-Repeated"); len(got) != 2 || got[0] != "first" || got[1] != "second" {
		t.Errorf("repeated headers = %v", got)
	}
	if body, err := os.ReadFile(path); err != nil || string(body) != "subscription body" {
		t.Fatalf("body = %q, error = %v", body, err)
	}
	// The original API still downloads the same body without returning metadata.
	if err := controller.DownloadUrlToFile(server.URL, "proxy", `{"X-Test":"request"}`, path, 1000); err != nil {
		t.Fatal(err)
	}
}

func TestDownloadUrlToFileFailureDoesNotPublishHeadersOrBody(t *testing.T) {
	controller := testHTTPController(t)
	for _, status := range []int{http.StatusForbidden, http.StatusPartialContent, http.StatusOK} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Profile-Title", "must not escape")
				w.Header().Set("Content-Length", "100")
				w.WriteHeader(status)
				_, _ = w.Write([]byte("truncated"))
			}))
			defer server.Close()
			directory := t.TempDir()
			path := filepath.Join(directory, "subscription")
			if err := os.WriteFile(path, []byte("original"), 0600); err != nil {
				t.Fatal(err)
			}
			headers, err := controller.DownloadUrlToFileWithHeaders(server.URL, "proxy", "", path, 1000)
			if err == nil || headers != "" {
				t.Fatalf("headers = %q, error = %v", headers, err)
			}
			if body, err := os.ReadFile(path); err != nil || string(body) != "original" {
				t.Fatalf("previous body = %q, error = %v", body, err)
			}
			files, err := os.ReadDir(directory)
			if err != nil || len(files) != 1 {
				t.Fatalf("temporary files leaked: %v, error = %v", files, err)
			}
		})
	}
}

func testHTTPController(t *testing.T) *CoreController {
	t.Helper()
	// A blocked default outbound makes successful downloads prove that the
	// requested outbound is selected, including after an HTTP redirect.
	config, err := coreserial.LoadJSONConfig(strings.NewReader(`{"log":{"loglevel":"none"},"outbounds":[{"protocol":"blackhole","tag":"blocked"},{"protocol":"freedom","tag":"proxy"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	inst, err := core.New(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = inst.Close() })
	if err := inst.Start(); err != nil {
		t.Fatal(err)
	}
	return &CoreController{coreInstance: inst, IsRunning: true}
}
