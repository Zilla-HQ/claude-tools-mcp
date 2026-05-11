package tools

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRawRead_BasicFunctionality(t *testing.T) {
	t.Run("returns raw bytes without line-number prefix", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "test.txt")
		content := "Line 1\nLine 2\nLine 3"
		require.NoError(t, os.WriteFile(path, []byte(content), 0o644))

		result, _, err := RawRead(context.Background(), &sdk.CallToolRequest{}, RawReadInput{Target: path})
		require.NoError(t, err)
		require.Len(t, result.Content, 1)
		text := result.Content[0].(*sdk.TextContent).Text
		// Should be the exact file contents — no "     1→" prefix.
		assert.Equal(t, content, text)
		assert.NotContains(t, text, "→")
	})

	t.Run("absolute path works", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "abs.txt")
		require.NoError(t, os.WriteFile(path, []byte("hello"), 0o644))

		result, _, err := RawRead(context.Background(), &sdk.CallToolRequest{}, RawReadInput{Target: path})
		require.NoError(t, err)
		assert.Equal(t, "hello", result.Content[0].(*sdk.TextContent).Text)
	})

	t.Run("relative path resolved against /workspace", func(t *testing.T) {
		// We can't write to /workspace in tests, so we just verify the error
		// points to /workspace/<path>, not ./path.
		_, _, err := RawRead(context.Background(), &sdk.CallToolRequest{}, RawReadInput{Target: "nonexistent/file.txt"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "/workspace/nonexistent/file.txt")
	})

	t.Run("nonexistent file returns error", func(t *testing.T) {
		_, _, err := RawRead(context.Background(), &sdk.CallToolRequest{}, RawReadInput{Target: "/tmp/does-not-exist-zilla-test.txt"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "does not exist")
	})

	t.Run("directory returns error", func(t *testing.T) {
		tmpDir := t.TempDir()
		_, _, err := RawRead(context.Background(), &sdk.CallToolRequest{}, RawReadInput{Target: tmpDir})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "directory")
	})

	t.Run("empty file returns empty string", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "empty.txt")
		require.NoError(t, os.WriteFile(path, []byte(""), 0o644))

		result, _, err := RawRead(context.Background(), &sdk.CallToolRequest{}, RawReadInput{Target: path})
		require.NoError(t, err)
		assert.Equal(t, "", result.Content[0].(*sdk.TextContent).Text)
	})

	t.Run("binary file returns raw bytes as string", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "bin.bin")
		data := []byte{0x00, 0x01, 0x02, 0xFF}
		require.NoError(t, os.WriteFile(path, data, 0o644))

		result, _, err := RawRead(context.Background(), &sdk.CallToolRequest{}, RawReadInput{Target: path})
		require.NoError(t, err)
		assert.Equal(t, string(data), result.Content[0].(*sdk.TextContent).Text)
	})
}

func TestRawRead_ContrastWithRead(t *testing.T) {
	// Confirm raw_read and read return different output for the same file.
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.txt")
	content := "hello\nworld"
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))

	state := NewState()
	readResult, err := state.executeRead(context.Background(), path, 0, 0)
	require.NoError(t, err)

	rawResult, _, err := RawRead(context.Background(), &sdk.CallToolRequest{}, RawReadInput{Target: path})
	require.NoError(t, err)

	rawText := rawResult.Content[0].(*sdk.TextContent).Text

	// read adds line numbers; raw_read does not
	assert.Contains(t, readResult, "     1→hello")
	assert.Equal(t, content, rawText)
	assert.NotEqual(t, readResult, rawText)
}
