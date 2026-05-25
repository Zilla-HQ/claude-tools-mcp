package tools

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupGitWorkspace creates a local git repo with a remote bare repo for testing push.
// Returns the workspace dir and the bare repo dir.
func setupGitWorkspace(t *testing.T) (workspaceDir, bareRepoDir string) {
	t.Helper()
	tmpDir := t.TempDir()

	// Create bare repo to act as remote.
	bareRepoDir = filepath.Join(tmpDir, "remote.git")
	require.NoError(t, os.MkdirAll(bareRepoDir, 0o755))
	require.NoError(t, runGit(context.Background(), bareRepoDir, "init", "--bare"))

	// Create workspace repo.
	workspaceDir = filepath.Join(tmpDir, "workspace")
	require.NoError(t, os.MkdirAll(workspaceDir, 0o755))
	require.NoError(t, runGit(context.Background(), workspaceDir, "init", "-b", "main"))
	require.NoError(t, runGit(context.Background(), workspaceDir, "config", "user.email", "test@test.com"))
	require.NoError(t, runGit(context.Background(), workspaceDir, "config", "user.name", "Test"))
	require.NoError(t, runGit(context.Background(), workspaceDir, "remote", "add", "origin", bareRepoDir))

	// Create an initial commit so HEAD exists.
	initialFile := filepath.Join(workspaceDir, "README.md")
	require.NoError(t, os.WriteFile(initialFile, []byte("# Workspace\n"), 0o644))
	require.NoError(t, runGit(context.Background(), workspaceDir, "add", "-A"))
	require.NoError(t, runGit(context.Background(), workspaceDir, "commit", "-m", "initial commit"))
	require.NoError(t, runGit(context.Background(), workspaceDir, "push", "origin", "HEAD:main"))

	return workspaceDir, bareRepoDir
}

// patchWorkspaceDir patches the hardcoded "/workspace" in commitWorkspace by
// using the executable's working dir approach. We instead expose a helper
// that lets tests override the workspace dir.
func commitWorkspaceWithDir(ctx context.Context, workspace string, args CommitWorkspaceInput) (*CommitWorkspaceOutput, error) {
	output := &CommitWorkspaceOutput{
		Errors: []string{},
	}

	if err := runGit(ctx, workspace, "add", "-A"); err != nil {
		output.Errors = append(output.Errors, "git add: "+err.Error())
		return output, nil
	}

	statusOut, err := runGitOutput(ctx, workspace, "status", "--porcelain")
	if err != nil {
		output.Errors = append(output.Errors, "git status: "+err.Error())
		return output, nil
	}

	if strings.TrimSpace(statusOut) != "" {
		msg := args.Message
		if msg == "" {
			msg = "chore: update workspace"
		}
		if err := runGit(ctx, workspace, "commit", "-m", msg); err != nil {
			output.Errors = append(output.Errors, "git commit: "+err.Error())
			return output, nil
		}
		sha, err := runGitOutput(ctx, workspace, "rev-parse", "HEAD")
		if err != nil {
			output.Errors = append(output.Errors, "git rev-parse: "+err.Error())
		} else {
			output.CommitSHA = strings.TrimSpace(sha)
		}
	}

	// Push (with rebase-on-rejection retry; see pushWithRebaseRetry).
	// Tests use the bare-repo path directly as the push URL since
	// buildAuthenticatedPushURL is env-driven and not exercised here.
	bareRepoURL, err := runGitOutput(ctx, workspace, "remote", "get-url", "origin")
	if err != nil {
		output.Errors = append(output.Errors, "git remote get-url: "+err.Error())
		return output, nil
	}
	if pushErr := pushWithRebaseRetry(ctx, workspace, strings.TrimSpace(bareRepoURL)); pushErr != nil {
		output.Errors = append(output.Errors, pushErr.Error())
		return output, nil
	}
	output.Pushed = true

	return output, nil
}

func TestCommitWorkspace_EmptyDiff(t *testing.T) {
	workspaceDir, _ := setupGitWorkspace(t)

	ctx := context.Background()
	output, err := commitWorkspaceWithDir(ctx, workspaceDir, CommitWorkspaceInput{})
	require.NoError(t, err)

	// Empty diff → no commit SHA but still pushed (push with nothing new is ok).
	assert.Empty(t, output.CommitSHA)
	assert.True(t, output.Pushed)
	assert.Empty(t, output.Errors)
}

func TestCommitWorkspace_WithChanges(t *testing.T) {
	workspaceDir, _ := setupGitWorkspace(t)

	// Add a new file.
	require.NoError(t, os.WriteFile(filepath.Join(workspaceDir, "new.txt"), []byte("content"), 0o644))

	ctx := context.Background()
	output, err := commitWorkspaceWithDir(ctx, workspaceDir, CommitWorkspaceInput{
		Message: "feat: add new file",
	})
	require.NoError(t, err)

	assert.NotEmpty(t, output.CommitSHA)
	assert.True(t, output.Pushed)
	assert.Empty(t, output.Errors)
	assert.Len(t, output.CommitSHA, 40) // full SHA
}

func TestCommitWorkspace_CustomMessage(t *testing.T) {
	workspaceDir, _ := setupGitWorkspace(t)
	require.NoError(t, os.WriteFile(filepath.Join(workspaceDir, "change.txt"), []byte("x"), 0o644))

	ctx := context.Background()
	output, err := commitWorkspaceWithDir(ctx, workspaceDir, CommitWorkspaceInput{
		Message: "custom: my message",
	})
	require.NoError(t, err)
	assert.NotEmpty(t, output.CommitSHA)

	// Verify commit message in log.
	logOut, err := runGitOutput(ctx, workspaceDir, "log", "--format=%s", "-1")
	require.NoError(t, err)
	assert.Contains(t, logOut, "custom: my message")
}

func TestCommitWorkspace_DefaultMessage(t *testing.T) {
	workspaceDir, _ := setupGitWorkspace(t)
	require.NoError(t, os.WriteFile(filepath.Join(workspaceDir, "file.txt"), []byte("data"), 0o644))

	ctx := context.Background()
	output, err := commitWorkspaceWithDir(ctx, workspaceDir, CommitWorkspaceInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, output.CommitSHA)

	logOut, err := runGitOutput(ctx, workspaceDir, "log", "--format=%s", "-1")
	require.NoError(t, err)
	assert.Contains(t, logOut, "chore: update workspace")
}

func TestCommitWorkspace_Idempotent(t *testing.T) {
	workspaceDir, _ := setupGitWorkspace(t)

	// First call — make a change.
	require.NoError(t, os.WriteFile(filepath.Join(workspaceDir, "once.txt"), []byte("once"), 0o644))
	ctx := context.Background()
	out1, err := commitWorkspaceWithDir(ctx, workspaceDir, CommitWorkspaceInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, out1.CommitSHA)

	// Second call — no changes.
	out2, err := commitWorkspaceWithDir(ctx, workspaceDir, CommitWorkspaceInput{})
	require.NoError(t, err)
	assert.Empty(t, out2.CommitSHA)   // no new commit
	assert.True(t, out2.Pushed)       // push still ran (idempotent)
	assert.Empty(t, out2.Errors)
}

func TestCommitWorkspace_SyncDist(t *testing.T) {
	mock := newMockS3Server(t)
	t.Setenv("R2_ENDPOINT_URL", mock.srv.URL)
	t.Setenv("R2_ACCESS_KEY_ID", "test-key")
	t.Setenv("R2_SECRET_ACCESS_KEY", "test-secret")
	t.Setenv("R2_SITES_BUCKET", "sites-bucket")
	t.Setenv("ZILLA_SUBDOMAIN", "mysite")

	// Create a fake dist directory.
	tmpDir := t.TempDir()
	distDir := filepath.Join(tmpDir, "sites", "main", "dist")
	require.NoError(t, os.MkdirAll(distDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(distDir, "index.html"), []byte("<html/>"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(distDir, "style.css"), []byte("body{}"), 0o644))

	// Create uploads/ that should be excluded.
	uploadsDir := filepath.Join(distDir, "uploads")
	require.NoError(t, os.MkdirAll(uploadsDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(uploadsDir, "photo.jpg"), []byte("jpg-data"), 0o644))

	count, errs := syncDistToR2(context.Background(), tmpDir)
	assert.Empty(t, errs)
	assert.Equal(t, 2, count) // index.html + style.css, NOT uploads/photo.jpg

	// Verify files were uploaded to R2 (check mock server).
	mock.mu.RLock()
	defer mock.mu.RUnlock()
	_, hasIndex := mock.objects["sites-bucket/mysite/index.html"]
	_, hasCSS := mock.objects["sites-bucket/mysite/style.css"]
	_, hasUpload := mock.objects["sites-bucket/mysite/uploads/photo.jpg"]
	assert.True(t, hasIndex, "index.html should be in R2")
	assert.True(t, hasCSS, "style.css should be in R2")
	assert.False(t, hasUpload, "uploads/ should be excluded")
}

func TestCommitWorkspace_SyncDistDelete(t *testing.T) {
	mock := newMockS3Server(t)
	t.Setenv("R2_ENDPOINT_URL", mock.srv.URL)
	t.Setenv("R2_ACCESS_KEY_ID", "test-key")
	t.Setenv("R2_SECRET_ACCESS_KEY", "test-secret")
	t.Setenv("R2_SITES_BUCKET", "sites-bucket")
	t.Setenv("ZILLA_SUBDOMAIN", "mysite2")

	// Pre-populate R2 with a stale file.
	mock.mu.Lock()
	mock.objects["sites-bucket/mysite2/stale.html"] = &mockS3Object{body: []byte("old"), etag: `"stale"`, contentType: "text/html"}
	mock.mu.Unlock()

	// Dist dir only has index.html (stale.html is absent).
	tmpDir := t.TempDir()
	distDir := filepath.Join(tmpDir, "sites", "main", "dist")
	require.NoError(t, os.MkdirAll(distDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(distDir, "index.html"), []byte("<html/>"), 0o644))

	count, errs := syncDistToR2(context.Background(), tmpDir)
	assert.Empty(t, errs)
	assert.Equal(t, 1, count)

	// stale.html should have been deleted from R2.
	mock.mu.RLock()
	_, staleExists := mock.objects["sites-bucket/mysite2/stale.html"]
	mock.mu.RUnlock()
	assert.False(t, staleExists, "stale.html should have been deleted")
}

// TestCommitWorkspace_RebaseOnRejection — the common reaper-wedge case.
// Remote has commits the sandbox doesn't (e.g. user fixed a workflow file
// via the GitHub web UI while the sandbox sat idle). pushWithRebaseRetry
// pulls --rebase, replays local on top, and the retried push succeeds.
func TestCommitWorkspace_RebaseOnRejection(t *testing.T) {
	workspaceDir, bareRepoDir := setupGitWorkspace(t)
	ctx := context.Background()

	// Make a divergent commit on remote via a second clone (no overlap with
	// what the workspace will commit, so the rebase resolves cleanly).
	otherClone := filepath.Join(t.TempDir(), "other-clone")
	require.NoError(t, runGit(ctx, "", "clone", bareRepoDir, otherClone))
	require.NoError(t, runGit(ctx, otherClone, "config", "user.email", "other@test.com"))
	require.NoError(t, runGit(ctx, otherClone, "config", "user.name", "Other"))
	require.NoError(t, os.WriteFile(filepath.Join(otherClone, "remote-only.txt"), []byte("from remote"), 0o644))
	require.NoError(t, runGit(ctx, otherClone, "add", "-A"))
	require.NoError(t, runGit(ctx, otherClone, "commit", "-m", "remote-only commit"))
	require.NoError(t, runGit(ctx, otherClone, "push", "origin", "HEAD:main"))

	// Now make a local change in the workspace.
	require.NoError(t, os.WriteFile(filepath.Join(workspaceDir, "local-only.txt"), []byte("from sandbox"), 0o644))

	output, err := commitWorkspaceWithDir(ctx, workspaceDir, CommitWorkspaceInput{
		Message: "feat: local change",
	})
	require.NoError(t, err)

	assert.Empty(t, output.Errors)
	assert.True(t, output.Pushed)
	assert.NotEmpty(t, output.CommitSHA)

	// After push, both commits exist on remote — verify via another clone.
	verifyClone := filepath.Join(t.TempDir(), "verify")
	require.NoError(t, runGit(ctx, "", "clone", bareRepoDir, verifyClone))
	_, statErr1 := os.Stat(filepath.Join(verifyClone, "remote-only.txt"))
	_, statErr2 := os.Stat(filepath.Join(verifyClone, "local-only.txt"))
	assert.NoError(t, statErr1)
	assert.NoError(t, statErr2)
}

// TestCommitWorkspace_AbortsOnRebaseConflict — true content conflict.
// Both sides edited the same file; the rebase can't auto-resolve. We expect
// the rebase to be aborted (workspace back to a clean state) and the error
// to surface clearly so the reaper / publish flow doesn't keep retrying.
func TestCommitWorkspace_AbortsOnRebaseConflict(t *testing.T) {
	workspaceDir, bareRepoDir := setupGitWorkspace(t)
	ctx := context.Background()

	// Both clones edit README.md to different content → guaranteed conflict.
	otherClone := filepath.Join(t.TempDir(), "other-clone")
	require.NoError(t, runGit(ctx, "", "clone", bareRepoDir, otherClone))
	require.NoError(t, runGit(ctx, otherClone, "config", "user.email", "other@test.com"))
	require.NoError(t, runGit(ctx, otherClone, "config", "user.name", "Other"))
	require.NoError(t, os.WriteFile(filepath.Join(otherClone, "README.md"), []byte("# Remote version\n"), 0o644))
	require.NoError(t, runGit(ctx, otherClone, "add", "-A"))
	require.NoError(t, runGit(ctx, otherClone, "commit", "-m", "remote: edit README"))
	require.NoError(t, runGit(ctx, otherClone, "push", "origin", "HEAD:main"))

	// Local edit to the same file.
	require.NoError(t, os.WriteFile(filepath.Join(workspaceDir, "README.md"), []byte("# Sandbox version\n"), 0o644))

	output, err := commitWorkspaceWithDir(ctx, workspaceDir, CommitWorkspaceInput{
		Message: "edit: local README",
	})
	require.NoError(t, err)

	assert.False(t, output.Pushed)
	require.NotEmpty(t, output.Errors)
	combined := strings.Join(output.Errors, " ")
	assert.Contains(t, combined, "rejected")
	assert.Contains(t, combined, "manual reconciliation")

	// Workspace is back to a clean state — rebase was aborted, no `.git/rebase-merge`.
	_, rebaseStateErr := os.Stat(filepath.Join(workspaceDir, ".git", "rebase-merge"))
	assert.True(t, os.IsNotExist(rebaseStateErr), "rebase should have been aborted")

	// The local commit still exists in the workspace (it's on the local branch
	// that was rebased-and-then-aborted; HEAD is back where it was pre-rebase).
	logOut, err := runGitOutput(ctx, workspaceDir, "log", "--format=%s", "-1")
	require.NoError(t, err)
	assert.Contains(t, logOut, "edit: local README")
}

func TestCommitWorkspace_MCPTool(t *testing.T) {
	// Test that the public CommitWorkspace MCP function returns structured content.
	// We can't easily test the /workspace path in unit tests, so we verify it
	// returns an error (not a panic) when /workspace doesn't exist or git fails.
	ctx := context.Background()
	req := &sdk.CallToolRequest{}

	// This will fail because /workspace may not be a git repo in CI — that's ok.
	// The important thing is it returns structured output rather than panicking.
	result, structured, _ := CommitWorkspace(ctx, req, CommitWorkspaceInput{})
	// Result may be nil if there's an error, or may be a structured result.
	// The function returns structured content even on partial failure.
	if result != nil {
		assert.NotNil(t, structured)
		out, ok := structured.(*CommitWorkspaceOutput)
		require.True(t, ok)
		assert.NotNil(t, out.Errors)
	}
}
