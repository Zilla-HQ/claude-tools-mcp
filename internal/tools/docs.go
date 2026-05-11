package tools

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

// docBucketKey returns the full S3 key for a doc_key under the company docs prefix.
func docBucketKey(companyID, docKey string) string {
	return companyID + "/docs/" + docKey
}

// ── read_doc ──────────────────────────────────────────────────────────────────

var ReadDocTool = sdk.Tool{
	Name:        "read_doc",
	Description: "Reads a private document from R2 storage (company data bucket, docs/ prefix). Returns body, ETag, and content-type.",
}

type ReadDocInput struct {
	DocKey string `json:"doc_key" jsonschema:"Relative document key (e.g. 'pages/about.html')"`
}

type ReadDocOutput struct {
	Body        string `json:"body"`
	ETag        string `json:"etag"`
	ContentType string `json:"content_type"`
}

func ReadDoc(ctx context.Context, req *sdk.CallToolRequest, args ReadDocInput) (*sdk.CallToolResult, any, error) {
	client, err := r2Client()
	if err != nil {
		return nil, nil, err
	}
	bucket, err := mustEnv("R2_COMPANY_DATA_BUCKET")
	if err != nil {
		return nil, nil, err
	}
	companyID, err := mustEnv("ZILLA_COMPANY_ID")
	if err != nil {
		return nil, nil, err
	}

	key := docBucketKey(companyID, args.DocKey)
	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("read_doc: %w", err)
	}
	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("read_doc: reading body: %w", err)
	}

	output := &ReadDocOutput{
		Body:        string(data),
		ETag:        derefString(out.ETag),
		ContentType: derefString(out.ContentType),
	}
	text := fmt.Sprintf("doc_key=%s etag=%s content_type=%s bytes=%d", args.DocKey, output.ETag, output.ContentType, len(data))
	return &sdk.CallToolResult{
		Content:           []sdk.Content{&sdk.TextContent{Text: text}},
		StructuredContent: output,
	}, output, nil
}

// ── write_doc ─────────────────────────────────────────────────────────────────

var WriteDocTool = sdk.Tool{
	Name:        "write_doc",
	Description: "Writes a private document to R2 storage (company data bucket, docs/ prefix). Use expected_etag=\"\" for create-only, a string ETag for conditional update, or omit for force-overwrite.",
}

type WriteDocInput struct {
	DocKey       string  `json:"doc_key" jsonschema:"Relative document key"`
	Body         string  `json:"body" jsonschema:"Document contents to upload"`
	ExpectedETag *string `json:"expected_etag,omitempty" jsonschema:"Optional ETag for conditional write. \"\"=create-only (IfNoneMatch:*), string=update-only (IfMatch), omit=force overwrite"`
}

type WriteDocOutput struct {
	ETag string `json:"etag"`
}

func WriteDoc(ctx context.Context, req *sdk.CallToolRequest, args WriteDocInput) (*sdk.CallToolResult, any, error) {
	client, err := r2Client()
	if err != nil {
		return nil, nil, err
	}
	bucket, err := mustEnv("R2_COMPANY_DATA_BUCKET")
	if err != nil {
		return nil, nil, err
	}
	companyID, err := mustEnv("ZILLA_COMPANY_ID")
	if err != nil {
		return nil, nil, err
	}

	key := docBucketKey(companyID, args.DocKey)
	input := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader([]byte(args.Body)),
	}
	if args.ExpectedETag != nil {
		if *args.ExpectedETag == "" {
			input.IfNoneMatch = aws.String("*")
		} else {
			input.IfMatch = args.ExpectedETag
		}
	}

	out, err := client.PutObject(ctx, input)
	if err != nil {
		if isETagMismatch(err) {
			etag := ""
			if args.ExpectedETag != nil {
				etag = *args.ExpectedETag
			}
			return nil, nil, &ETagMismatchError{Expected: etag}
		}
		return nil, nil, fmt.Errorf("write_doc: %w", err)
	}

	output := &WriteDocOutput{ETag: derefString(out.ETag)}
	text := fmt.Sprintf("doc written: doc_key=%s etag=%s", args.DocKey, output.ETag)
	return &sdk.CallToolResult{
		Content:           []sdk.Content{&sdk.TextContent{Text: text}},
		StructuredContent: output,
	}, output, nil
}

// ── list_docs ─────────────────────────────────────────────────────────────────

var ListDocsTool = sdk.Tool{
	Name:        "list_docs",
	Description: "Lists private documents in R2 storage (company data bucket, docs/ prefix) with optional sub-prefix filter.",
}

type ListDocsInput struct {
	Prefix string `json:"prefix,omitempty" jsonschema:"Optional sub-prefix to filter doc keys"`
}

type DocItem struct {
	DocKey string `json:"doc_key"`
	ETag   string `json:"etag"`
	Size   int64  `json:"size"`
}

type ListDocsOutput struct {
	Docs []DocItem `json:"docs"`
}

func ListDocs(ctx context.Context, req *sdk.CallToolRequest, args ListDocsInput) (*sdk.CallToolResult, any, error) {
	client, err := r2Client()
	if err != nil {
		return nil, nil, err
	}
	bucket, err := mustEnv("R2_COMPANY_DATA_BUCKET")
	if err != nil {
		return nil, nil, err
	}
	companyID, err := mustEnv("ZILLA_COMPANY_ID")
	if err != nil {
		return nil, nil, err
	}

	prefix := companyID + "/docs/"
	if args.Prefix != "" {
		prefix = prefix + args.Prefix
	}

	objects, err := listBucketObjects(ctx, client, bucket, prefix)
	if err != nil {
		return nil, nil, fmt.Errorf("list_docs: %w", err)
	}

	companyDocsPrefix := companyID + "/docs/"
	items := make([]DocItem, 0, len(objects))
	for _, obj := range objects {
		fullKey := derefString(obj.Key)
		docKey := fullKey
		if len(fullKey) > len(companyDocsPrefix) {
			docKey = fullKey[len(companyDocsPrefix):]
		}
		items = append(items, DocItem{
			DocKey: docKey,
			ETag:   derefString(obj.ETag),
			Size:   derefInt64(obj.Size),
		})
	}

	output := &ListDocsOutput{Docs: items}
	text := fmt.Sprintf("listed %d docs under prefix %q", len(items), prefix)
	return &sdk.CallToolResult{
		Content:           []sdk.Content{&sdk.TextContent{Text: text}},
		StructuredContent: output,
	}, output, nil
}

// ── delete_doc ────────────────────────────────────────────────────────────────

var DeleteDocTool = sdk.Tool{
	Name:        "delete_doc",
	Description: "Deletes a private document from R2 storage (company data bucket, docs/ prefix). Optionally provide expected_etag for conditional delete.",
}

type DeleteDocInput struct {
	DocKey       string  `json:"doc_key" jsonschema:"Relative document key to delete"`
	ExpectedETag *string `json:"expected_etag,omitempty" jsonschema:"Optional ETag for conditional delete (IfMatch)"`
}

type DeleteDocOutput struct {
	OK bool `json:"ok"`
}

func DeleteDoc(ctx context.Context, req *sdk.CallToolRequest, args DeleteDocInput) (*sdk.CallToolResult, any, error) {
	client, err := r2Client()
	if err != nil {
		return nil, nil, err
	}
	bucket, err := mustEnv("R2_COMPANY_DATA_BUCKET")
	if err != nil {
		return nil, nil, err
	}
	companyID, err := mustEnv("ZILLA_COMPANY_ID")
	if err != nil {
		return nil, nil, err
	}

	key := docBucketKey(companyID, args.DocKey)
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	if args.ExpectedETag != nil {
		input.IfMatch = args.ExpectedETag
	}

	_, err = client.DeleteObject(ctx, input)
	if err != nil {
		if isETagMismatch(err) {
			return nil, nil, &ETagMismatchError{Expected: derefString(args.ExpectedETag)}
		}
		return nil, nil, fmt.Errorf("delete_doc: %w", err)
	}

	output := &DeleteDocOutput{OK: true}
	text := fmt.Sprintf("deleted doc: %s", args.DocKey)
	return &sdk.CallToolResult{
		Content:           []sdk.Content{&sdk.TextContent{Text: text}},
		StructuredContent: output,
	}, output, nil
}
