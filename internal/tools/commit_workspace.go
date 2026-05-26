package tools

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

var CommitWorkspaceTool = sdk.Tool{
	Name:        "commit_workspace",
	Description: "Stages all changes in /workspace, commits if non-empty, and pushes to origin HEAD:main. With sync_dist=true, also syncs dist/ to R2 sites bucket (excluding uploads/, with --delete semantics). Idempotent: empty diffs are no-ops.",
}

type CommitWorkspaceInput struct {
	Message  string `json:"message,omitempty" jsonschema:"Optional git commit message"`
	SyncDist bool   `json:"sync_dist,omitempty" jsonschema:"If true, sync dist/ to R2 sites bucket after commit"`
	// Optional fresh GitHub installation token used for the push. Overrides
	// the ZILLA_GH_REPO_TOKEN env var (which is set at sandbox boot and
	// expires after 1h). The platform-side commitSandbox mints a new token
	// per call and passes it here so long-idle sandboxes can still push.
	GhToken string `json:"gh_token,omitempty" jsonschema:"Optional override for the GitHub installation token used to authenticate the push. When omitted falls back to ZILLA_GH_REPO_TOKEN env."`
}

type CommitWorkspaceOutput struct {
	CommitSHA       string   `json:"commit_sha,omitempty"`
	Pushed          bool     `json:"pushed"`
	DistSyncedFiles int      `json:"dist_synced_files"`
	Errors          []string `json:"errors"`
}

func CommitWorkspace(ctx context.Context, req *sdk.CallToolRequest, args CommitWorkspaceInput) (*sdk.CallToolResult, any, error) {
	output := &CommitWorkspaceOutput{
		Errors: []string{},
	}

	workspace := "/workspace"

	// ── git stage ────────────────────────────────────────────────────────────
	if err := runGit(ctx, workspace, "add", "-A"); err != nil {
		output.Errors = append(output.Errors, fmt.Sprintf("git add: %v", err))
		return commitWorkspaceResult(output)
	}

	// ── check if there's anything to commit ──────────────────────────────────
	statusOut, err := runGitOutput(ctx, workspace, "status", "--porcelain")
	if err != nil {
		output.Errors = append(output.Errors, fmt.Sprintf("git status: %v", err))
		return commitWorkspaceResult(output)
	}

	if strings.TrimSpace(statusOut) != "" {
		// There are staged changes — commit.
		msg := args.Message
		if msg == "" {
			msg = "chore: update workspace"
		}
		// Resolve the GitHub App bot identity that owns the installation
		// token so the commit author matches the pusher. Vercel rejects
		// commits whose author isn't associated with the GitHub App
		// installation (INC-406). Failure is non-fatal — we fall back to
		// git's configured user.name/user.email.
		commitArgs := buildCommitArgs(ctx, args.GhToken, msg)
		if err := runGit(ctx, workspace, commitArgs...); err != nil {
			output.Errors = append(output.Errors, fmt.Sprintf("git commit: %v", err))
			return commitWorkspaceResult(output)
		}

		// Get the commit SHA.
		sha, err := runGitOutput(ctx, workspace, "rev-parse", "HEAD")
		if err != nil {
			output.Errors = append(output.Errors, fmt.Sprintf("git rev-parse: %v", err))
		} else {
			output.CommitSHA = strings.TrimSpace(sha)
		}
	}

	// ── push ─────────────────────────────────────────────────────────────────
	pushURL, err := buildAuthenticatedPushURL(workspace, args.GhToken)
	if err != nil {
		output.Errors = append(output.Errors, fmt.Sprintf("build push URL: %v", err))
		return commitWorkspaceResult(output)
	}

	if pushErr := pushWithRebaseRetry(ctx, workspace, pushURL); pushErr != nil {
		output.Errors = append(output.Errors, pushErr.Error())
		return commitWorkspaceResult(output)
	}
	output.Pushed = true

	// ── dist sync ─────────────────────────────────────────────────────────────
	if args.SyncDist {
		synced, syncErrs := syncDistToR2(ctx, workspace)
		output.DistSyncedFiles = synced
		output.Errors = append(output.Errors, syncErrs...)
	}

	return commitWorkspaceResult(output)
}

func commitWorkspaceResult(output *CommitWorkspaceOutput) (*sdk.CallToolResult, any, error) {
	// Platform-side commitSandbox in zilla calls JSON.parse on the Text body
	// and routes on errors[] + commit_sha. Serialize as JSON, not key=value
	// (the prior printf-style format caused "non-JSON" parse failures even
	// when the commit + push succeeded — see zilla INC-261 cold-boot validation).
	jsonBytes, err := json.Marshal(output)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal commit_workspace output: %w", err)
	}
	return &sdk.CallToolResult{
		Content:           []sdk.Content{&sdk.TextContent{Text: string(jsonBytes)}},
		StructuredContent: output,
	}, output, nil
}

// pushWithRebaseRetry pushes HEAD to main, integrating remote changes via
// `pull --rebase` when the initial push is rejected as non-fast-forward.
//
// The retry handles the common case where the company repo was edited
// out-of-band (e.g., the user fixed a workflow file via the GitHub web UI
// while the sandbox sat idle) — the agent's local commits are clean atop
// remote and rebase succeeds without intervention.
//
// On true content conflicts (both sides edited the same file) the rebase
// aborts and the function returns a clear error. The caller (the idle
// reaper or the agent-driven publish flow) should NOT auto-retry — the
// sandbox is in a state only a human can resolve safely.
func pushWithRebaseRetry(ctx context.Context, workspace, pushURL string) error {
	pushArgs := []string{"push", pushURL, "HEAD:main"}
	pushErr := runGit(ctx, workspace, pushArgs...)
	if pushErr == nil {
		return nil
	}

	// Detect non-fast-forward rejection. git emits "rejected" + "fetch first"
	// (or "non-fast-forward") in stderr; runGit folds stderr into the
	// returned error. Anything else is a real failure (auth, network, etc.).
	errStr := pushErr.Error()
	if !strings.Contains(errStr, "rejected") && !strings.Contains(errStr, "non-fast-forward") {
		return fmt.Errorf("git push: %v", pushErr)
	}

	// Try to integrate remote via rebase. Use the authenticated push URL
	// directly rather than touching the configured origin remote.
	rebaseErr := runGit(ctx, workspace, "pull", pushURL, "main", "--rebase", "--no-edit")
	if rebaseErr != nil {
		// Likely a content conflict. Abort the rebase so the workspace is
		// in a clean state for the next attempt or for manual inspection.
		_ = runGit(ctx, workspace, "rebase", "--abort")
		return fmt.Errorf(
			"git push rejected; pull --rebase failed (likely conflict): %v. Sandbox needs manual reconciliation",
			rebaseErr,
		)
	}

	// Retry the push now that local is rebased on top of remote.
	if pushErr2 := runGit(ctx, workspace, pushArgs...); pushErr2 != nil {
		return fmt.Errorf("git push (after rebase): %v", pushErr2)
	}
	return nil
}

// runGit runs a git command in the given directory, discarding stdout.
func runGit(ctx context.Context, dir string, args ...string) error {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

// runGitOutput runs a git command and returns its stdout.
func runGitOutput(ctx context.Context, dir string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

// buildCommitArgs assembles `git commit` args, prefixed with -c user.name /
// -c user.email when we can resolve the GitHub App bot identity. The -c
// flags scope to the single git invocation, so we don't mutate workspace
// or global git config.
//
// If the identity can't be resolved (no token, env not set, API failure),
// we fall back to whatever the surrounding git config provides — the commit
// still succeeds, but Vercel may reject the push downstream (INC-406).
func buildCommitArgs(ctx context.Context, ghToken string, msg string) []string {
	args := []string{}
	if name, email, err := resolveBotIdentity(ctx, resolveToken(ghToken)); err == nil {
		args = append(args, "-c", "user.name="+name, "-c", "user.email="+email)
	}
	args = append(args, "commit", "-m", msg)
	return args
}

// resolveToken returns the GitHub installation token to use, preferring the
// per-call override and falling back to the env var set at sandbox boot.
func resolveToken(override string) string {
	if override != "" {
		return override
	}
	return os.Getenv("ZILLA_GH_REPO_TOKEN")
}

// botIdentity holds the GitHub App bot identity that owns an installation token.
type botIdentity struct {
	name  string
	email string
}

var (
	botIdentityCache   = map[string]botIdentity{}
	botIdentityCacheMu sync.Mutex

	// githubAPIBase is the base URL for the GitHub API. Overridable in tests.
	githubAPIBase = "https://api.github.com"

	// botIdentityHTTPClient is the HTTP client used to resolve the bot
	// identity. Exposed as a var so tests can swap in a short-timeout client.
	botIdentityHTTPClient = &http.Client{Timeout: 10 * time.Second}
)

// resolveBotIdentity returns the GitHub App bot's name + noreply email for
// the given installation token. The bot identity for a GitHub App is what
// Vercel (and `git log`, `gh pr view`, etc.) attribute the commit to — it
// must match the App installation that pushed the commit, or Vercel will
// reject the push and the project will never deploy (INC-406).
//
// Resolution order:
//  1. ZILLA_GH_BOT_NAME + ZILLA_GH_BOT_EMAIL env vars (platform escape hatch
//     for environments where the API call is undesirable or for tests).
//  2. GET /user with the installation token → {login, id}. The standard
//     noreply email pattern is <id>+<login>@users.noreply.github.com.
//
// Returns an error when no token is available or the API call fails;
// callers fall back to whatever git's user.name/user.email config provides.
func resolveBotIdentity(ctx context.Context, token string) (string, string, error) {
	if name, email := os.Getenv("ZILLA_GH_BOT_NAME"), os.Getenv("ZILLA_GH_BOT_EMAIL"); name != "" && email != "" {
		return name, email, nil
	}
	if token == "" {
		return "", "", fmt.Errorf("no GitHub token available to resolve bot identity")
	}

	botIdentityCacheMu.Lock()
	if id, ok := botIdentityCache[token]; ok {
		botIdentityCacheMu.Unlock()
		return id.name, id.email, nil
	}
	botIdentityCacheMu.Unlock()

	req, err := http.NewRequestWithContext(ctx, "GET", githubAPIBase+"/user", nil)
	if err != nil {
		return "", "", fmt.Errorf("build /user request: %w", err)
	}
	req.Header.Set("Authorization", "token "+token)
	req.Header.Set("Accept", "application/vnd.github+json")

	resp, err := botIdentityHTTPClient.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("GET /user: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return "", "", fmt.Errorf("GET /user: status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var u struct {
		Login string `json:"login"`
		ID    int64  `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&u); err != nil {
		return "", "", fmt.Errorf("decode /user: %w", err)
	}
	if u.Login == "" || u.ID == 0 {
		return "", "", fmt.Errorf("GET /user returned empty login or id")
	}

	identity := botIdentity{
		name:  u.Login,
		email: fmt.Sprintf("%d+%s@users.noreply.github.com", u.ID, u.Login),
	}

	botIdentityCacheMu.Lock()
	botIdentityCache[token] = identity
	botIdentityCacheMu.Unlock()

	return identity.name, identity.email, nil
}

// buildAuthenticatedPushURL injects an installation token into the remote URL
// for push auth. The override argument wins when non-empty — used by the
// platform-side commitSandbox to pass a freshly-minted token, since the
// env-supplied ZILLA_GH_REPO_TOKEN is set at sandbox boot and expires after 1h
// (long-idle sandboxes can't push with the stale env token).
func buildAuthenticatedPushURL(workspaceDir string, override string) (string, error) {
	token := resolveToken(override)
	repoURL := os.Getenv("ZILLA_GH_REPO_URL")
	if repoURL == "" {
		// Fall back to configured remote origin.
		remoteURL, err := runGitOutput(context.Background(), workspaceDir, "remote", "get-url", "origin")
		if err != nil {
			return "", fmt.Errorf("no ZILLA_GH_REPO_URL and cannot get remote: %w", err)
		}
		repoURL = strings.TrimSpace(remoteURL)
	}

	if token == "" {
		// No token — push as-is (may fail if auth required).
		return repoURL, nil
	}

	// Inject token into the URL: https://TOKEN@github.com/org/repo.git
	u, err := url.Parse(repoURL)
	if err != nil {
		return "", fmt.Errorf("parse repo URL %q: %w", repoURL, err)
	}
	u.User = url.UserPassword("x-access-token", token)
	return u.String(), nil
}

// syncDistToR2 walks dist/ and uploads changed files to R2, then deletes
// keys in R2 that no longer exist locally (excluding uploads/*). Returns count and errors.
func syncDistToR2(ctx context.Context, workspaceDir string) (int, []string) {
	var errs []string

	sitesBucket := os.Getenv("R2_SITES_BUCKET")
	if sitesBucket == "" {
		return 0, []string{"R2_SITES_BUCKET is not set"}
	}
	subdomain := os.Getenv("ZILLA_SUBDOMAIN")
	if subdomain == "" {
		return 0, []string{"ZILLA_SUBDOMAIN is not set"}
	}

	client, err := r2Client()
	if err != nil {
		return 0, []string{fmt.Sprintf("r2Client: %v", err)}
	}

	distDir := filepath.Join(workspaceDir, "dist")
	if _, err := os.Stat(distDir); os.IsNotExist(err) {
		return 0, []string{fmt.Sprintf("dist dir does not exist: %s", distDir)}
	}

	r2Prefix := subdomain + "/"

	// Build set of local files (relative to distDir).
	localFiles := map[string]string{} // relative path → full path
	err = filepath.WalkDir(distDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(distDir, path)
		// Use forward slashes for S3 keys.
		rel = filepath.ToSlash(rel)
		// Skip uploads/
		if strings.HasPrefix(rel, "uploads/") {
			return nil
		}
		localFiles[rel] = path
		return nil
	})
	if err != nil {
		return 0, []string{fmt.Sprintf("walk dist dir: %v", err)}
	}

	// Upload all local files concurrently.
	var uploaded atomic.Int64
	var mu sync.Mutex
	sem := make(chan struct{}, 16) // max 16 concurrent uploads
	var wg sync.WaitGroup

	for rel, fullPath := range localFiles {
		wg.Add(1)
		go func(rel, fullPath string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			data, readErr := os.ReadFile(fullPath)
			if readErr != nil {
				mu.Lock()
				errs = append(errs, fmt.Sprintf("read %s: %v", rel, readErr))
				mu.Unlock()
				return
			}

			s3Key := r2Prefix + rel
			_, putErr := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket:      aws.String(sitesBucket),
				Key:         aws.String(s3Key),
				Body:        bytes.NewReader(data),
				ContentType: aws.String(detectContentType(rel)),
			})
			if putErr != nil {
				mu.Lock()
				errs = append(errs, fmt.Sprintf("upload %s: %v", rel, putErr))
				mu.Unlock()
				return
			}
			uploaded.Add(1)
		}(rel, fullPath)
	}
	wg.Wait()

	// Delete R2 objects that no longer exist locally (excluding uploads/).
	r2Objects, listErr := listBucketObjects(ctx, client, sitesBucket, r2Prefix)
	if listErr != nil {
		errs = append(errs, fmt.Sprintf("list R2 for delete: %v", listErr))
	} else {
		for _, obj := range r2Objects {
			fullKey := derefString(obj.Key)
			// Strip r2Prefix to get relative path.
			rel := ""
			if len(fullKey) > len(r2Prefix) {
				rel = fullKey[len(r2Prefix):]
			}
			// Skip uploads/.
			if strings.HasPrefix(rel, "uploads/") {
				continue
			}
			if _, exists := localFiles[rel]; !exists {
				_, delErr := client.DeleteObject(ctx, &s3.DeleteObjectInput{
					Bucket: aws.String(sitesBucket),
					Key:    aws.String(fullKey),
				})
				if delErr != nil {
					errs = append(errs, fmt.Sprintf("delete stale %s: %v", rel, delErr))
				}
			}
		}
	}

	return int(uploaded.Load()), errs
}

// detectContentType returns a basic content-type based on file extension.
func detectContentType(name string) string {
	ext := strings.ToLower(filepath.Ext(name))
	switch ext {
	case ".html", ".htm":
		return "text/html; charset=utf-8"
	case ".css":
		return "text/css; charset=utf-8"
	case ".js", ".mjs":
		return "application/javascript"
	case ".json":
		return "application/json"
	case ".png":
		return "image/png"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".gif":
		return "image/gif"
	case ".svg":
		return "image/svg+xml"
	case ".webp":
		return "image/webp"
	case ".ico":
		return "image/x-icon"
	case ".woff":
		return "font/woff"
	case ".woff2":
		return "font/woff2"
	case ".ttf":
		return "font/ttf"
	case ".txt":
		return "text/plain; charset=utf-8"
	case ".xml":
		return "application/xml"
	case ".webmanifest":
		return "application/manifest+json"
	default:
		return "application/octet-stream"
	}
}
