package tools

import (
	"context"
	"testing"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setUserdataEnv(t *testing.T, serverURL string) {
	t.Helper()
	t.Setenv("R2_ENDPOINT_URL", serverURL)
	t.Setenv("R2_ACCESS_KEY_ID", "test-key")
	t.Setenv("R2_SECRET_ACCESS_KEY", "test-secret")
	t.Setenv("R2_COMPANY_DATA_BUCKET", "data-bucket")
	t.Setenv("ZILLA_COMPANY_ID", "company-789")
}

func TestUserdata_RoundTrip(t *testing.T) {
	mock := newMockS3Server(t)
	setUserdataEnv(t, mock.srv.URL)

	ctx := context.Background()
	req := &sdk.CallToolRequest{}

	t.Run("write then read", func(t *testing.T) {
		out, _, err := WriteUserdata(ctx, req, WriteUserdataInput{UserdataKey: "uploads/avatar.png", Body: "fake-png"})
		require.NoError(t, err)
		structured := out.StructuredContent.(*WriteUserdataOutput)
		assert.NotEmpty(t, structured.ETag)

		readOut, _, err := ReadUserdata(ctx, req, ReadUserdataInput{UserdataKey: "uploads/avatar.png"})
		require.NoError(t, err)
		readStructured := readOut.StructuredContent.(*ReadUserdataOutput)
		assert.Equal(t, "fake-png", readStructured.Body)
		assert.Equal(t, structured.ETag, readStructured.ETag)
	})

	t.Run("list userdata", func(t *testing.T) {
		_, _, err := WriteUserdata(ctx, req, WriteUserdataInput{UserdataKey: "files/doc.pdf", Body: "pdf-data"})
		require.NoError(t, err)
		_, _, err = WriteUserdata(ctx, req, WriteUserdataInput{UserdataKey: "files/doc2.pdf", Body: "pdf-data-2"})
		require.NoError(t, err)

		listOut, _, err := ListUserdata(ctx, req, ListUserdataInput{})
		require.NoError(t, err)
		listStructured := listOut.StructuredContent.(*ListUserdataOutput)
		assert.GreaterOrEqual(t, len(listStructured.Items), 2)
		for _, item := range listStructured.Items {
			assert.NotEmpty(t, item.UserdataKey)
		}
	})

	t.Run("delete userdata", func(t *testing.T) {
		out, _, err := WriteUserdata(ctx, req, WriteUserdataInput{UserdataKey: "temp/remove.txt", Body: "bye"})
		require.NoError(t, err)
		etag := out.StructuredContent.(*WriteUserdataOutput).ETag

		delOut, _, err := DeleteUserdata(ctx, req, DeleteUserdataInput{UserdataKey: "temp/remove.txt", ExpectedETag: &etag})
		require.NoError(t, err)
		assert.True(t, delOut.StructuredContent.(*DeleteUserdataOutput).OK)

		_, _, err = ReadUserdata(ctx, req, ReadUserdataInput{UserdataKey: "temp/remove.txt"})
		require.Error(t, err)
	})

	t.Run("no public_url in userdata output", func(t *testing.T) {
		out, _, err := WriteUserdata(ctx, req, WriteUserdataInput{UserdataKey: "check/type.txt", Body: "x"})
		require.NoError(t, err)
		assert.IsType(t, &WriteUserdataOutput{}, out.StructuredContent)
	})
}

func TestUserdata_ETagConditional(t *testing.T) {
	mock := newMockS3Server(t)
	setUserdataEnv(t, mock.srv.URL)

	ctx := context.Background()
	req := &sdk.CallToolRequest{}

	t.Run("create-only fails if exists", func(t *testing.T) {
		emptyStr := ""
		_, _, err := WriteUserdata(ctx, req, WriteUserdataInput{UserdataKey: "once/file.bin", Body: "v1", ExpectedETag: &emptyStr})
		require.NoError(t, err)

		_, _, err = WriteUserdata(ctx, req, WriteUserdataInput{UserdataKey: "once/file.bin", Body: "v2", ExpectedETag: &emptyStr})
		require.Error(t, err)
		var etagErr *ETagMismatchError
		assert.ErrorAs(t, err, &etagErr)
	})

	t.Run("stale etag fails", func(t *testing.T) {
		_, _, err := WriteUserdata(ctx, req, WriteUserdataInput{UserdataKey: "versioned/file.bin", Body: "v1"})
		require.NoError(t, err)

		stale := `"wrong"`
		_, _, err = WriteUserdata(ctx, req, WriteUserdataInput{UserdataKey: "versioned/file.bin", Body: "v2", ExpectedETag: &stale})
		require.Error(t, err)
		var etagErr *ETagMismatchError
		assert.ErrorAs(t, err, &etagErr)
	})
}
