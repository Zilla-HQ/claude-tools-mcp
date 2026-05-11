package tools

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

var RawReadTool = sdk.Tool{
	Name:        "raw_read",
	Description: "Reads a file and returns raw bytes without any line-number prefix. Unlike the read tool, this returns the exact file contents. Resolves target relative to /workspace if not absolute.",
}

type RawReadInput struct {
	Target string `json:"target" jsonschema:"The path to the file to read. Resolved relative to /workspace if not absolute."`
}

type RawReadOutput struct {
	Content string `json:"content"`
}

func RawRead(ctx context.Context, req *sdk.CallToolRequest, args RawReadInput) (*sdk.CallToolResult, any, error) {
	resolved := args.Target
	if !filepath.IsAbs(resolved) {
		resolved = filepath.Join("/workspace", resolved)
	}
	resolved = filepath.Clean(resolved)

	fileInfo, err := os.Stat(resolved)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, fmt.Errorf("file does not exist: %s", resolved)
		}
		return nil, nil, fmt.Errorf("cannot stat file: %s", err)
	}
	if fileInfo.IsDir() {
		return nil, nil, fmt.Errorf("target is a directory, not a file: %s", resolved)
	}

	content, err := os.ReadFile(resolved)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot read file: %s", err)
	}

	result := string(content)
	output := &RawReadOutput{Content: result}
	return &sdk.CallToolResult{
		Content:           []sdk.Content{&sdk.TextContent{Text: result}},
		StructuredContent: output,
	}, output, nil
}
