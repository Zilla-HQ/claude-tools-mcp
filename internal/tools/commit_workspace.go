package tools

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

var CommitWorkspaceTool = sdk.Tool{
	Name:        "commit_workspace",
	Description: "Stages all changes in /workspace, commits if non-empty, and pushes to origin HEAD:main. With sync_dist=true, also syncs sites/main/dist/ to R2 sites bucket (excluding uploads/, with --delete semantics). Idempotent: empty diffs are no-ops.",
}

type CommitWorkspaceInput struct {
	Message  string `json:"message,omitempty" jsonschema:"Optional git commit message"`
	SyncDist bool   `json:"sync_dist,omitempty" jsonschema:"If true, sync sites/main/dist/ to R2 sites bucket after commit"`
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
		if err := runGit(ctx, workspace, "commit", "-m", msg); err != nil {
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
	pushURL, err := buildAuthenticatedPushURL(workspace)
	if err != nil {
		output.Errors = append(output.Errors, fmt.Sprintf("build push URL: %v", err))
		return commitWorkspaceResult(output)
	}

	pushArgs := []string{"push", pushURL, "HEAD:main"}
	if err := runGit(ctx, workspace, pushArgs...); err != nil {
		output.Errors = append(output.Errors, fmt.Sprintf("git push: %v", err))
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

// buildAuthenticatedPushURL injects ZILLA_GH_REPO_TOKEN into the remote URL for push auth.
func buildAuthenticatedPushURL(workspaceDir string) (string, error) {
	token := os.Getenv("ZILLA_GH_REPO_TOKEN")
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

// syncDistToR2 walks sites/main/dist/ and uploads changed files to R2, then deletes
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

	distDir := filepath.Join(workspaceDir, "sites", "main", "dist")
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
