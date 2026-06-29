package tools

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

// INC-1640: events authenticate with the per-sandbox bearer token, not the old
// shared-secret HMAC. postEvent must send X-Zilla-Token + X-Zilla-Company (and
// X-Job-Id) and must NOT send the legacy X-Sandbox-Signature header.
func TestPostEvent_SendsBearerHeaders(t *testing.T) {
	var gotToken, gotCompany, gotJob, gotSandbox, gotSig string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotToken = r.Header.Get("X-Zilla-Token")
		gotCompany = r.Header.Get("X-Zilla-Company")
		gotJob = r.Header.Get("X-Job-Id")
		gotSandbox = r.Header.Get("X-Sandbox-Id")
		gotSig = r.Header.Get("X-Sandbox-Signature")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ok := postEvent(srv.URL, "tok-abc", "company-123", "sandbox-1", "job-9", []byte(`{"type":"event","seq":1}`))

	assert.True(t, ok, "2xx response should report delivered")
	assert.Equal(t, "tok-abc", gotToken, "X-Zilla-Token must carry ZILLA_AUTH_TOKEN")
	assert.Equal(t, "company-123", gotCompany, "X-Zilla-Company must carry ZILLA_COMPANY_ID")
	assert.Equal(t, "job-9", gotJob)
	assert.Equal(t, "sandbox-1", gotSandbox)
	assert.Empty(t, gotSig, "legacy HMAC signature header must no longer be sent")
}

func TestPostEvent_Non2xxIsNotDelivered(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	ok := postEvent(srv.URL, "tok", "company", "sandbox", "job", []byte(`{"seq":1}`))
	assert.False(t, ok, "a non-2xx response must not count as delivered")
}

func TestPostEvent_UnreachableIsNotDelivered(t *testing.T) {
	// Closed server → connection refused → postEvent returns false (caller retries).
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := srv.URL
	srv.Close()

	ok := postEvent(url, "tok", "company", "sandbox", "job", []byte(`{"seq":1}`))
	assert.False(t, ok)
}
