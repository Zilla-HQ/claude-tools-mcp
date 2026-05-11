package tools

import (
	"context"
	"os"
	"testing"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setAssetEnv configures env vars for asset tool tests pointing at the mock server.
func setAssetEnv(t *testing.T, serverURL string) {
	t.Helper()
	t.Setenv("R2_ENDPOINT_URL", serverURL)
	t.Setenv("R2_ACCESS_KEY_ID", "test-key")
	t.Setenv("R2_SECRET_ACCESS_KEY", "test-secret")
	t.Setenv("R2_COMPANY_PUBLIC_BUCKET", "test-bucket")
	t.Setenv("ZILLA_COMPANY_ID", "company-123")
	t.Setenv("R2_PUBLIC_ASSETS_BASE_URL", "https://assets.example.com")
}

func TestAssets_RoundTrip(t *testing.T) {
	mock := newMockS3Server(t)
	setAssetEnv(t, mock.srv.URL)

	ctx := context.Background()
	req := &sdk.CallToolRequest{}

	t.Run("write then read", func(t *testing.T) {
		// Write an asset.
		writeOut, _, err := WriteAsset(ctx, req, WriteAssetInput{
			AssetKey: "images/hero.jpg",
			Body:     "fake image data",
		})
		require.NoError(t, err)
		structured := writeOut.StructuredContent.(*WriteAssetOutput)
		assert.NotEmpty(t, structured.ETag)
		assert.Equal(t, "https://assets.example.com/company-123/images/hero.jpg", structured.PublicURL)

		// Read it back.
		readOut, _, err := ReadAsset(ctx, req, ReadAssetInput{AssetKey: "images/hero.jpg"})
		require.NoError(t, err)
		readStructured := readOut.StructuredContent.(*ReadAssetOutput)
		assert.Equal(t, "fake image data", readStructured.Body)
		assert.Equal(t, structured.ETag, readStructured.ETag)
		assert.Equal(t, "https://assets.example.com/company-123/images/hero.jpg", readStructured.PublicURL)
	})

	t.Run("list assets", func(t *testing.T) {
		// Write two more assets.
		_, _, err := WriteAsset(ctx, req, WriteAssetInput{AssetKey: "fonts/main.woff2", Body: "font"})
		require.NoError(t, err)
		_, _, err = WriteAsset(ctx, req, WriteAssetInput{AssetKey: "css/style.css", Body: "body{}"})
		require.NoError(t, err)

		listOut, _, err := ListAssets(ctx, req, ListAssetsInput{})
		require.NoError(t, err)
		listStructured := listOut.StructuredContent.(*ListAssetsOutput)
		assert.GreaterOrEqual(t, len(listStructured.Assets), 3)

		// All should have public URLs.
		for _, item := range listStructured.Assets {
			assert.Contains(t, item.PublicURL, "https://assets.example.com/company-123/")
			assert.NotEmpty(t, item.AssetKey)
		}
	})

	t.Run("list with prefix filter", func(t *testing.T) {
		listOut, _, err := ListAssets(ctx, req, ListAssetsInput{Prefix: "images/"})
		require.NoError(t, err)
		listStructured := listOut.StructuredContent.(*ListAssetsOutput)
		for _, item := range listStructured.Assets {
			assert.True(t, len(item.AssetKey) == 0 || item.AssetKey == "images/hero.jpg",
				"expected only images/ assets, got %s", item.AssetKey)
		}
	})

	t.Run("delete asset", func(t *testing.T) {
		// Write, then delete.
		writeOut, _, err := WriteAsset(ctx, req, WriteAssetInput{AssetKey: "temp/delete-me.txt", Body: "bye"})
		require.NoError(t, err)
		etag := writeOut.StructuredContent.(*WriteAssetOutput).ETag

		// Delete with correct etag.
		delOut, _, err := DeleteAsset(ctx, req, DeleteAssetInput{AssetKey: "temp/delete-me.txt", ExpectedETag: &etag})
		require.NoError(t, err)
		assert.True(t, delOut.StructuredContent.(*DeleteAssetOutput).OK)

		// Reading should now fail.
		_, _, err = ReadAsset(ctx, req, ReadAssetInput{AssetKey: "temp/delete-me.txt"})
		require.Error(t, err)
	})
}

func TestAssets_ETagConditional(t *testing.T) {
	mock := newMockS3Server(t)
	setAssetEnv(t, mock.srv.URL)

	ctx := context.Background()
	req := &sdk.CallToolRequest{}

	t.Run("create-only fails if object already exists", func(t *testing.T) {
		// First write succeeds.
		emptyStr := ""
		_, _, err := WriteAsset(ctx, req, WriteAssetInput{
			AssetKey:     "once/only.txt",
			Body:         "original",
			ExpectedETag: &emptyStr, // create-only
		})
		require.NoError(t, err)

		// Second write should fail (object exists).
		_, _, err = WriteAsset(ctx, req, WriteAssetInput{
			AssetKey:     "once/only.txt",
			Body:         "duplicate",
			ExpectedETag: &emptyStr,
		})
		require.Error(t, err)
		var etagErr *ETagMismatchError
		assert.ErrorAs(t, err, &etagErr)
	})

	t.Run("IfMatch fails with stale etag", func(t *testing.T) {
		// Write initial version.
		out, _, err := WriteAsset(ctx, req, WriteAssetInput{AssetKey: "versioned/file.txt", Body: "v1"})
		require.NoError(t, err)
		_ = out

		// Attempt to update with wrong etag.
		staleETag := `"stale-etag"`
		_, _, err = WriteAsset(ctx, req, WriteAssetInput{
			AssetKey:     "versioned/file.txt",
			Body:         "v2",
			ExpectedETag: &staleETag,
		})
		require.Error(t, err)
		var etagErr *ETagMismatchError
		assert.ErrorAs(t, err, &etagErr)
	})

	t.Run("IfMatch succeeds with correct etag", func(t *testing.T) {
		out, _, err := WriteAsset(ctx, req, WriteAssetInput{AssetKey: "matched/file.txt", Body: "v1"})
		require.NoError(t, err)
		etag := out.StructuredContent.(*WriteAssetOutput).ETag

		_, _, err = WriteAsset(ctx, req, WriteAssetInput{
			AssetKey:     "matched/file.txt",
			Body:         "v2",
			ExpectedETag: &etag,
		})
		require.NoError(t, err)
	})

	t.Run("delete with wrong etag fails", func(t *testing.T) {
		out, _, err := WriteAsset(ctx, req, WriteAssetInput{AssetKey: "del-etag/file.txt", Body: "data"})
		require.NoError(t, err)
		_ = out

		staleETag := `"wrong"`
		_, _, err = DeleteAsset(ctx, req, DeleteAssetInput{AssetKey: "del-etag/file.txt", ExpectedETag: &staleETag})
		require.Error(t, err)
		var etagErr *ETagMismatchError
		assert.ErrorAs(t, err, &etagErr)
	})
}

func TestAssets_MissingEnv(t *testing.T) {
	// Clear all relevant env vars.
	for _, k := range []string{"R2_ENDPOINT_URL", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY",
		"R2_COMPANY_PUBLIC_BUCKET", "ZILLA_COMPANY_ID", "R2_PUBLIC_ASSETS_BASE_URL"} {
		t.Setenv(k, "")
	}

	ctx := context.Background()
	req := &sdk.CallToolRequest{}

	_, _, err := ReadAsset(ctx, req, ReadAssetInput{AssetKey: "test.txt"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "R2_ENDPOINT_URL")

	os.Setenv("R2_ENDPOINT_URL", "http://localhost:9999")
	os.Setenv("R2_ACCESS_KEY_ID", "k")
	os.Setenv("R2_SECRET_ACCESS_KEY", "s")
	_, _, err = ReadAsset(ctx, req, ReadAssetInput{AssetKey: "test.txt"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "R2_COMPANY_PUBLIC_BUCKET")
}
