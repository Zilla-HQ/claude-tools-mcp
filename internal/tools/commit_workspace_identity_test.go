package tools

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// resetBotIdentityCache clears the per-token identity cache between tests.
// The package-level cache leaks between sub-tests otherwise (a previous
// test's token would resolve from cache instead of re-hitting the mock).
func resetBotIdentityCache(t *testing.T) {
	t.Helper()
	botIdentityCacheMu.Lock()
	botIdentityCache = map[string]botIdentity{}
	botIdentityCacheMu.Unlock()
}

// newMockGitHubAPI stands in for api.github.com. Returns the server and a
// counter of /user hits so tests can assert caching behavior.
func newMockGitHubAPI(t *testing.T, login string, id int64) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var hits atomic.Int64
	mux := http.NewServeMux()
	mux.HandleFunc("/user", func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if got := r.Header.Get("Authorization"); !strings.HasPrefix(got, "token ") {
			http.Error(w, "missing token", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"login":"` + login + `","id":` + itoa(id) + `}`))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, &hits
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

func withMockAPI(t *testing.T, baseURL string) {
	t.Helper()
	prev := githubAPIBase
	githubAPIBase = baseURL
	t.Cleanup(func() { githubAPIBase = prev })
}

func TestResolveBotIdentity_EnvOverride(t *testing.T) {
	resetBotIdentityCache(t)
	t.Setenv("ZILLA_GH_BOT_NAME", "my-app[bot]")
	t.Setenv("ZILLA_GH_BOT_EMAIL", "987+my-app[bot]@users.noreply.github.com")

	name, email, err := resolveBotIdentity(context.Background(), "anything")
	require.NoError(t, err)
	assert.Equal(t, "my-app[bot]", name)
	assert.Equal(t, "987+my-app[bot]@users.noreply.github.com", email)
}

func TestResolveBotIdentity_NoToken(t *testing.T) {
	resetBotIdentityCache(t)
	// Make sure env override isn't set from a leaked previous test.
	t.Setenv("ZILLA_GH_BOT_NAME", "")
	t.Setenv("ZILLA_GH_BOT_EMAIL", "")

	_, _, err := resolveBotIdentity(context.Background(), "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no GitHub token")
}

func TestResolveBotIdentity_APILookup(t *testing.T) {
	resetBotIdentityCache(t)
	t.Setenv("ZILLA_GH_BOT_NAME", "")
	t.Setenv("ZILLA_GH_BOT_EMAIL", "")
	srv, hits := newMockGitHubAPI(t, "zilla-platform[bot]", 12345)
	withMockAPI(t, srv.URL)

	name, email, err := resolveBotIdentity(context.Background(), "ghs_token_abc")
	require.NoError(t, err)
	assert.Equal(t, "zilla-platform[bot]", name)
	assert.Equal(t, "12345+zilla-platform[bot]@users.noreply.github.com", email)
	assert.Equal(t, int64(1), hits.Load())

	// Second call with same token should hit cache, not the API.
	name2, email2, err := resolveBotIdentity(context.Background(), "ghs_token_abc")
	require.NoError(t, err)
	assert.Equal(t, name, name2)
	assert.Equal(t, email, email2)
	assert.Equal(t, int64(1), hits.Load(), "second call should be served from cache")

	// Different token must trigger a fresh lookup.
	_, _, err = resolveBotIdentity(context.Background(), "ghs_token_xyz")
	require.NoError(t, err)
	assert.Equal(t, int64(2), hits.Load())
}

func TestResolveBotIdentity_APIError(t *testing.T) {
	resetBotIdentityCache(t)
	t.Setenv("ZILLA_GH_BOT_NAME", "")
	t.Setenv("ZILLA_GH_BOT_EMAIL", "")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "bad credentials", http.StatusUnauthorized)
	}))
	t.Cleanup(srv.Close)
	withMockAPI(t, srv.URL)

	_, _, err := resolveBotIdentity(context.Background(), "ghs_bad")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "401")
}

// TestCommitWorkspace_CommitAuthor_FromEnv — the smallest end-to-end check
// that bot identity actually lands on the commit object. INC-406 was caused
// by the commit author being "Zilla Platform"; this test verifies the new
// path overrides it with the GH App bot identity.
func TestCommitWorkspace_CommitAuthor_FromEnv(t *testing.T) {
	resetBotIdentityCache(t)
	workspaceDir, _ := setupGitWorkspace(t)
	require.NoError(t, os.WriteFile(filepath.Join(workspaceDir, "new.txt"), []byte("x"), 0o644))

	t.Setenv("ZILLA_GH_BOT_NAME", "zilla-platform[bot]")
	t.Setenv("ZILLA_GH_BOT_EMAIL", "555+zilla-platform[bot]@users.noreply.github.com")

	ctx := context.Background()
	output, err := commitWorkspaceWithDir(ctx, workspaceDir, CommitWorkspaceInput{
		Message: "feat: bot-authored commit",
	})
	require.NoError(t, err)
	assert.Empty(t, output.Errors)
	require.NotEmpty(t, output.CommitSHA)

	authorLine, err := runGitOutput(ctx, workspaceDir, "log", "--format=%an <%ae>", "-1")
	require.NoError(t, err)
	assert.Contains(t, authorLine, "zilla-platform[bot] <555+zilla-platform[bot]@users.noreply.github.com>")

	// Also verify the workspace's git config is *not* mutated — the -c
	// flags should be scoped to the single commit invocation only.
	configName, err := runGitOutput(ctx, workspaceDir, "config", "user.name")
	require.NoError(t, err)
	assert.Equal(t, "Test", strings.TrimSpace(configName))
}

// TestCommitWorkspace_CommitAuthor_FallbackToGitConfig — no env, no token →
// commit still succeeds using whatever git config provides. The sandbox sets
// a default in entrypoint.sh, so the worst case is a "Zilla Platform"
// attributed commit (the pre-INC-406 behavior), not a hard failure.
func TestCommitWorkspace_CommitAuthor_FallbackToGitConfig(t *testing.T) {
	resetBotIdentityCache(t)
	workspaceDir, _ := setupGitWorkspace(t)
	require.NoError(t, os.WriteFile(filepath.Join(workspaceDir, "new.txt"), []byte("x"), 0o644))

	t.Setenv("ZILLA_GH_BOT_NAME", "")
	t.Setenv("ZILLA_GH_BOT_EMAIL", "")
	t.Setenv("ZILLA_GH_REPO_TOKEN", "")

	ctx := context.Background()
	output, err := commitWorkspaceWithDir(ctx, workspaceDir, CommitWorkspaceInput{})
	require.NoError(t, err)
	assert.Empty(t, output.Errors)
	require.NotEmpty(t, output.CommitSHA)

	// Falls back to setupGitWorkspace's "Test <test@test.com>".
	authorLine, err := runGitOutput(ctx, workspaceDir, "log", "--format=%an <%ae>", "-1")
	require.NoError(t, err)
	assert.Contains(t, authorLine, "Test <test@test.com>")
}

// TestCommitWorkspace_CommitAuthor_FromAPI — drives the full path: no env
// override, token present, mock GitHub API returns {login,id}; the resulting
// commit must be authored by the noreply identity derived from /user.
func TestCommitWorkspace_CommitAuthor_FromAPI(t *testing.T) {
	resetBotIdentityCache(t)
	t.Setenv("ZILLA_GH_BOT_NAME", "")
	t.Setenv("ZILLA_GH_BOT_EMAIL", "")
	srv, _ := newMockGitHubAPI(t, "zilla-staging[bot]", 7777)
	withMockAPI(t, srv.URL)

	workspaceDir, _ := setupGitWorkspace(t)
	require.NoError(t, os.WriteFile(filepath.Join(workspaceDir, "new.txt"), []byte("x"), 0o644))

	ctx := context.Background()
	output, err := commitWorkspaceWithDir(ctx, workspaceDir, CommitWorkspaceInput{
		Message: "feat: api-derived author",
		GhToken: "ghs_runtime_token",
	})
	require.NoError(t, err)
	assert.Empty(t, output.Errors)
	require.NotEmpty(t, output.CommitSHA)

	authorLine, err := runGitOutput(ctx, workspaceDir, "log", "--format=%an <%ae>", "-1")
	require.NoError(t, err)
	assert.Contains(t, authorLine, "zilla-staging[bot] <7777+zilla-staging[bot]@users.noreply.github.com>")
}
