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

// userdataBucketKey returns the full S3 key for a userdata_key under the company userdata prefix.
func userdataBucketKey(companyID, userdataKey string) string {
	return companyID + "/userdata/" + userdataKey
}

// ── read_userdata ─────────────────────────────────────────────────────────────

var ReadUserdataTool = sdk.Tool{
	Name:        "read_userdata",
	Description: "Reads a private user-uploaded blob from R2 storage (company data bucket, userdata/ prefix). Returns body, ETag, and content-type.",
}

type ReadUserdataInput struct {
	UserdataKey string `json:"userdata_key" jsonschema:"Relative userdata key (e.g. 'uploads/photo.jpg')"`
}

type ReadUserdataOutput struct {
	Body        string `json:"body"`
	ETag        string `json:"etag"`
	ContentType string `json:"content_type"`
}

func ReadUserdata(ctx context.Context, req *sdk.CallToolRequest, args ReadUserdataInput) (*sdk.CallToolResult, any, error) {
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

	key := userdataBucketKey(companyID, args.UserdataKey)
	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("read_userdata: %w", err)
	}
	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("read_userdata: reading body: %w", err)
	}

	output := &ReadUserdataOutput{
		Body:        string(data),
		ETag:        derefString(out.ETag),
		ContentType: derefString(out.ContentType),
	}
	text := fmt.Sprintf("userdata_key=%s etag=%s content_type=%s bytes=%d", args.UserdataKey, output.ETag, output.ContentType, len(data))
	return &sdk.CallToolResult{
		Content:           []sdk.Content{&sdk.TextContent{Text: text}},
		StructuredContent: output,
	}, output, nil
}

// ── write_userdata ────────────────────────────────────────────────────────────

var WriteUserdataTool = sdk.Tool{
	Name:        "write_userdata",
	Description: "Writes a private user-uploaded blob to R2 storage (company data bucket, userdata/ prefix). Use expected_etag=\"\" for create-only, a string ETag for conditional update, or omit for force-overwrite.",
}

type WriteUserdataInput struct {
	UserdataKey  string  `json:"userdata_key" jsonschema:"Relative userdata key"`
	Body         string  `json:"body" jsonschema:"Blob contents to upload"`
	ExpectedETag *string `json:"expected_etag,omitempty" jsonschema:"Optional ETag for conditional write. \"\"=create-only (IfNoneMatch:*), string=update-only (IfMatch), omit=force overwrite"`
}

type WriteUserdataOutput struct {
	ETag string `json:"etag"`
}

func WriteUserdata(ctx context.Context, req *sdk.CallToolRequest, args WriteUserdataInput) (*sdk.CallToolResult, any, error) {
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

	key := userdataBucketKey(companyID, args.UserdataKey)
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
		return nil, nil, fmt.Errorf("write_userdata: %w", err)
	}

	output := &WriteUserdataOutput{ETag: derefString(out.ETag)}
	text := fmt.Sprintf("userdata written: userdata_key=%s etag=%s", args.UserdataKey, output.ETag)
	return &sdk.CallToolResult{
		Content:           []sdk.Content{&sdk.TextContent{Text: text}},
		StructuredContent: output,
	}, output, nil
}

// ── list_userdata ─────────────────────────────────────────────────────────────

var ListUserdataTool = sdk.Tool{
	Name:        "list_userdata",
	Description: "Lists private user-uploaded blobs in R2 storage (company data bucket, userdata/ prefix) with optional sub-prefix filter.",
}

type ListUserdataInput struct {
	Prefix string `json:"prefix,omitempty" jsonschema:"Optional sub-prefix to filter userdata keys"`
}

type UserdataItem struct {
	UserdataKey string `json:"userdata_key"`
	ETag        string `json:"etag"`
	Size        int64  `json:"size"`
}

type ListUserdataOutput struct {
	Items []UserdataItem `json:"items"`
}

func ListUserdata(ctx context.Context, req *sdk.CallToolRequest, args ListUserdataInput) (*sdk.CallToolResult, any, error) {
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

	prefix := companyID + "/userdata/"
	if args.Prefix != "" {
		prefix = prefix + args.Prefix
	}

	objects, err := listBucketObjects(ctx, client, bucket, prefix)
	if err != nil {
		return nil, nil, fmt.Errorf("list_userdata: %w", err)
	}

	companyUserdataPrefix := companyID + "/userdata/"
	items := make([]UserdataItem, 0, len(objects))
	for _, obj := range objects {
		fullKey := derefString(obj.Key)
		userdataKey := fullKey
		if len(fullKey) > len(companyUserdataPrefix) {
			userdataKey = fullKey[len(companyUserdataPrefix):]
		}
		items = append(items, UserdataItem{
			UserdataKey: userdataKey,
			ETag:        derefString(obj.ETag),
			Size:        derefInt64(obj.Size),
		})
	}

	output := &ListUserdataOutput{Items: items}
	text := fmt.Sprintf("listed %d userdata items under prefix %q", len(items), prefix)
	return &sdk.CallToolResult{
		Content:           []sdk.Content{&sdk.TextContent{Text: text}},
		StructuredContent: output,
	}, output, nil
}

// ── delete_userdata ───────────────────────────────────────────────────────────

var DeleteUserdataTool = sdk.Tool{
	Name:        "delete_userdata",
	Description: "Deletes a private user-uploaded blob from R2 storage (company data bucket, userdata/ prefix). Optionally provide expected_etag for conditional delete.",
}

type DeleteUserdataInput struct {
	UserdataKey  string  `json:"userdata_key" jsonschema:"Relative userdata key to delete"`
	ExpectedETag *string `json:"expected_etag,omitempty" jsonschema:"Optional ETag for conditional delete (IfMatch)"`
}

type DeleteUserdataOutput struct {
	OK bool `json:"ok"`
}

func DeleteUserdata(ctx context.Context, req *sdk.CallToolRequest, args DeleteUserdataInput) (*sdk.CallToolResult, any, error) {
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

	key := userdataBucketKey(companyID, args.UserdataKey)
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
		return nil, nil, fmt.Errorf("delete_userdata: %w", err)
	}

	output := &DeleteUserdataOutput{OK: true}
	text := fmt.Sprintf("deleted userdata: %s", args.UserdataKey)
	return &sdk.CallToolResult{
		Content:           []sdk.Content{&sdk.TextContent{Text: text}},
		StructuredContent: output,
	}, output, nil
}
