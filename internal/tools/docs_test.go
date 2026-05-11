package tools

import (
	"context"
	"testing"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setDocEnv(t *testing.T, serverURL string) {
	t.Helper()
	t.Setenv("R2_ENDPOINT_URL", serverURL)
	t.Setenv("R2_ACCESS_KEY_ID", "test-key")
	t.Setenv("R2_SECRET_ACCESS_KEY", "test-secret")
	t.Setenv("R2_COMPANY_DATA_BUCKET", "data-bucket")
	t.Setenv("ZILLA_COMPANY_ID", "company-456")
}

func TestDocs_RoundTrip(t *testing.T) {
	mock := newMockS3Server(t)
	setDocEnv(t, mock.srv.URL)

	ctx := context.Background()
	req := &sdk.CallToolRequest{}

	t.Run("write then read", func(t *testing.T) {
		out, _, err := WriteDoc(ctx, req, WriteDocInput{DocKey: "pages/about.html", Body: "<h1>About</h1>"})
		require.NoError(t, err)
		structured := out.StructuredContent.(*WriteDocOutput)
		assert.NotEmpty(t, structured.ETag)

		readOut, _, err := ReadDoc(ctx, req, ReadDocInput{DocKey: "pages/about.html"})
		require.NoError(t, err)
		readStructured := readOut.StructuredContent.(*ReadDocOutput)
		assert.Equal(t, "<h1>About</h1>", readStructured.Body)
		assert.Equal(t, structured.ETag, readStructured.ETag)
	})

	t.Run("no public_url in doc output", func(t *testing.T) {
		out, _, err := WriteDoc(ctx, req, WriteDocInput{DocKey: "private.txt", Body: "secret"})
		require.NoError(t, err)
		// WriteDocOutput has no PublicURL field.
		assert.IsType(t, &WriteDocOutput{}, out.StructuredContent)

		readOut, _, err := ReadDoc(ctx, req, ReadDocInput{DocKey: "private.txt"})
		require.NoError(t, err)
		assert.IsType(t, &ReadDocOutput{}, readOut.StructuredContent)
		// Ensure no public_url field exists (compile-time check via struct type).
	})

	t.Run("list docs", func(t *testing.T) {
		_, _, err := WriteDoc(ctx, req, WriteDocInput{DocKey: "blog/post1.md", Body: "# Post 1"})
		require.NoError(t, err)
		_, _, err = WriteDoc(ctx, req, WriteDocInput{DocKey: "blog/post2.md", Body: "# Post 2"})
		require.NoError(t, err)

		listOut, _, err := ListDocs(ctx, req, ListDocsInput{})
		require.NoError(t, err)
		listStructured := listOut.StructuredContent.(*ListDocsOutput)
		assert.GreaterOrEqual(t, len(listStructured.Docs), 2)
		for _, d := range listStructured.Docs {
			assert.NotEmpty(t, d.DocKey)
			assert.NotEmpty(t, d.ETag)
		}
	})

	t.Run("delete doc", func(t *testing.T) {
		out, _, err := WriteDoc(ctx, req, WriteDocInput{DocKey: "temp/delete-me.txt", Body: "bye"})
		require.NoError(t, err)
		etag := out.StructuredContent.(*WriteDocOutput).ETag

		delOut, _, err := DeleteDoc(ctx, req, DeleteDocInput{DocKey: "temp/delete-me.txt", ExpectedETag: &etag})
		require.NoError(t, err)
		assert.True(t, delOut.StructuredContent.(*DeleteDocOutput).OK)

		_, _, err = ReadDoc(ctx, req, ReadDocInput{DocKey: "temp/delete-me.txt"})
		require.Error(t, err)
	})
}

func TestDocs_ETagConditional(t *testing.T) {
	mock := newMockS3Server(t)
	setDocEnv(t, mock.srv.URL)

	ctx := context.Background()
	req := &sdk.CallToolRequest{}

	t.Run("create-only fails if exists", func(t *testing.T) {
		emptyStr := ""
		_, _, err := WriteDoc(ctx, req, WriteDocInput{DocKey: "once/doc.md", Body: "v1", ExpectedETag: &emptyStr})
		require.NoError(t, err)

		_, _, err = WriteDoc(ctx, req, WriteDocInput{DocKey: "once/doc.md", Body: "v2", ExpectedETag: &emptyStr})
		require.Error(t, err)
		var etagErr *ETagMismatchError
		assert.ErrorAs(t, err, &etagErr)
	})

	t.Run("stale etag fails", func(t *testing.T) {
		_, _, err := WriteDoc(ctx, req, WriteDocInput{DocKey: "stale/doc.md", Body: "v1"})
		require.NoError(t, err)

		stale := `"wrong"`
		_, _, err = WriteDoc(ctx, req, WriteDocInput{DocKey: "stale/doc.md", Body: "v2", ExpectedETag: &stale})
		require.Error(t, err)
		var etagErr *ETagMismatchError
		assert.ErrorAs(t, err, &etagErr)
	})
}
