package tools

import (
	"crypto/md5"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// mockS3Object represents a stored object in the mock S3 server.
type mockS3Object struct {
	body        []byte
	etag        string
	contentType string
}

// mockS3Server is a minimal S3-compatible HTTP server for testing.
// Supports: GET, PUT, DELETE (single object), LIST (ListObjectsV2).
type mockS3Server struct {
	mu      sync.RWMutex
	objects map[string]*mockS3Object // key → object
	srv     *httptest.Server
}

func newMockS3Server(t *testing.T) *mockS3Server {
	t.Helper()
	m := &mockS3Server{
		objects: make(map[string]*mockS3Object),
	}
	m.srv = httptest.NewServer(m)
	t.Cleanup(m.srv.Close)
	return m
}

func (m *mockS3Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Path format: /<bucket>/<key...>
	// or with query: /<bucket>?list-type=2&prefix=...
	parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)
	if len(parts) < 2 || parts[1] == "" {
		// Bucket-level request — check for list query.
		if r.URL.Query().Get("list-type") == "2" {
			m.handleList(w, r, parts[0])
			return
		}
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	key := parts[0] + "/" + parts[1]
	switch r.Method {
	case http.MethodGet:
		m.handleGet(w, r, key)
	case http.MethodPut:
		m.handlePut(w, r, key)
	case http.MethodDelete:
		m.handleDelete(w, r, key)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (m *mockS3Server) handleGet(w http.ResponseWriter, r *http.Request, key string) {
	m.mu.RLock()
	obj, ok := m.objects[key]
	m.mu.RUnlock()
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`<Error><Code>NoSuchKey</Code></Error>`))
		return
	}
	w.Header().Set("ETag", obj.etag)
	w.Header().Set("Content-Type", obj.contentType)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(obj.body)
}

func (m *mockS3Server) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	// Conditional checks.
	ifMatch := r.Header.Get("If-Match")
	ifNoneMatch := r.Header.Get("If-None-Match")

	m.mu.Lock()
	defer m.mu.Unlock()

	existing, exists := m.objects[key]

	if ifNoneMatch == "*" && exists {
		w.WriteHeader(http.StatusPreconditionFailed)
		_, _ = w.Write([]byte(`<Error><Code>PreconditionFailed</Code></Error>`))
		return
	}
	if ifMatch != "" {
		if !exists || existing.etag != ifMatch {
			w.WriteHeader(http.StatusPreconditionFailed)
			_, _ = w.Write([]byte(`<Error><Code>PreconditionFailed</Code></Error>`))
			return
		}
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	ct := r.Header.Get("Content-Type")
	if ct == "" {
		ct = "application/octet-stream"
	}
	etag := fmt.Sprintf(`"%x"`, md5.Sum(body))
	m.objects[key] = &mockS3Object{body: body, etag: etag, contentType: ct}
	w.Header().Set("ETag", etag)
	w.WriteHeader(http.StatusOK)
}

func (m *mockS3Server) handleDelete(w http.ResponseWriter, r *http.Request, key string) {
	ifMatch := r.Header.Get("If-Match")

	m.mu.Lock()
	defer m.mu.Unlock()

	existing, exists := m.objects[key]
	if ifMatch != "" {
		if !exists || existing.etag != ifMatch {
			w.WriteHeader(http.StatusPreconditionFailed)
			_, _ = w.Write([]byte(`<Error><Code>PreconditionFailed</Code></Error>`))
			return
		}
	}
	delete(m.objects, key)
	w.WriteHeader(http.StatusNoContent)
}

type s3ListResult struct {
	XMLName               xml.Name `xml:"ListBucketResult"`
	IsTruncated           bool     `xml:"IsTruncated"`
	Contents              []s3Item `xml:"Contents"`
	NextContinuationToken string   `xml:"NextContinuationToken,omitempty"`
}

type s3Item struct {
	Key  string `xml:"Key"`
	ETag string `xml:"ETag"`
	Size int    `xml:"Size"`
}

func (m *mockS3Server) handleList(w http.ResponseWriter, r *http.Request, bucket string) {
	prefix := r.URL.Query().Get("prefix")

	m.mu.RLock()
	defer m.mu.RUnlock()

	var items []s3Item
	for key, obj := range m.objects {
		// Key stored as "bucket/rest" — check bucket matches.
		parts := strings.SplitN(key, "/", 2)
		if parts[0] != bucket {
			continue
		}
		fullKey := key // "bucket/rest" — but S3 uses just the key part without bucket.
		// Our key format is bucket+"/"+rest. The list prefix is applied to the object key
		// which in real S3 doesn't include the bucket. We store "bucket/key" so strip bucket.
		objectKey := ""
		if len(parts) > 1 {
			objectKey = parts[1]
		}
		if prefix != "" && !strings.HasPrefix(objectKey, prefix) {
			continue
		}
		_ = fullKey
		items = append(items, s3Item{
			Key:  objectKey,
			ETag: obj.etag,
			Size: len(obj.body),
		})
	}

	result := s3ListResult{
		IsTruncated: false,
		Contents:    items,
	}
	data, _ := xml.Marshal(result)
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>`))
	_, _ = w.Write(data)
}

// s3ClientFor creates a test S3 client pointed at the mock server with the given bucket.
func s3ClientFor(srv *mockS3Server) *s3TestEnv {
	return &s3TestEnv{
		client: r2ClientFromConfig(srv.srv.URL, "test-key", "test-secret"),
		bucket: "test-bucket",
	}
}

type s3TestEnv struct {
	client interface{} // *s3.Client but we don't use it directly in tests
	bucket string
}
