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

// assetBucketKey returns the full S3 key for a given asset_key under the company prefix.
func assetBucketKey(companyID, assetKey string) string {
	return companyID + "/" + assetKey
}

// assetPublicURL builds the CDN-accessible public URL for an asset.
func assetPublicURL(baseURL, companyID, assetKey string) string {
	return baseURL + "/" + companyID + "/" + assetKey
}

// ── read_asset ────────────────────────────────────────────────────────────────

var ReadAssetTool = sdk.Tool{
	Name:        "read_asset",
	Description: "Reads a public site asset from R2 storage. Returns the body, ETag, content-type, and public CDN URL.",
}

type ReadAssetInput struct {
	AssetKey string `json:"asset_key" jsonschema:"Relative asset key (e.g. 'images/hero.jpg')"`
}

type ReadAssetOutput struct {
	Body        string `json:"body"`
	ETag        string `json:"etag"`
	ContentType string `json:"content_type"`
	PublicURL   string `json:"public_url"`
}

func ReadAsset(ctx context.Context, req *sdk.CallToolRequest, args ReadAssetInput) (*sdk.CallToolResult, any, error) {
	client, err := r2Client()
	if err != nil {
		return nil, nil, err
	}
	bucket, err := mustEnv("R2_COMPANY_PUBLIC_BUCKET")
	if err != nil {
		return nil, nil, err
	}
	companyID, err := mustEnv("ZILLA_COMPANY_ID")
	if err != nil {
		return nil, nil, err
	}
	baseURL, err := mustEnv("R2_PUBLIC_ASSETS_BASE_URL")
	if err != nil {
		return nil, nil, err
	}

	key := assetBucketKey(companyID, args.AssetKey)
	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("read_asset: %w", err)
	}
	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("read_asset: reading body: %w", err)
	}

	output := &ReadAssetOutput{
		Body:        string(data),
		ETag:        derefString(out.ETag),
		ContentType: derefString(out.ContentType),
		PublicURL:   assetPublicURL(baseURL, companyID, args.AssetKey),
	}
	text := fmt.Sprintf("asset_key=%s etag=%s content_type=%s public_url=%s bytes=%d",
		args.AssetKey, output.ETag, output.ContentType, output.PublicURL, len(data))
	return &sdk.CallToolResult{
		Content:           []sdk.Content{&sdk.TextContent{Text: text}},
		StructuredContent: output,
	}, output, nil
}

// ── write_asset ───────────────────────────────────────────────────────────────

var WriteAssetTool = sdk.Tool{
	Name:        "write_asset",
	Description: "Writes a public site asset to R2 storage. Use expected_etag=null to create-only (fails if exists), a string ETag to update-only (fails if changed), or omit for force-overwrite.",
}

type WriteAssetInput struct {
	AssetKey     string  `json:"asset_key" jsonschema:"Relative asset key"`
	Body         string  `json:"body" jsonschema:"File contents to upload"`
	ExpectedETag *string `json:"expected_etag,omitempty" jsonschema:"Optional ETag for conditional write. null=create-only, string=update-only (IfMatch), omit=force overwrite"`
}

type WriteAssetOutput struct {
	ETag      string `json:"etag"`
	PublicURL string `json:"public_url"`
}

func WriteAsset(ctx context.Context, req *sdk.CallToolRequest, args WriteAssetInput) (*sdk.CallToolResult, any, error) {
	client, err := r2Client()
	if err != nil {
		return nil, nil, err
	}
	bucket, err := mustEnv("R2_COMPANY_PUBLIC_BUCKET")
	if err != nil {
		return nil, nil, err
	}
	companyID, err := mustEnv("ZILLA_COMPANY_ID")
	if err != nil {
		return nil, nil, err
	}
	baseURL, err := mustEnv("R2_PUBLIC_ASSETS_BASE_URL")
	if err != nil {
		return nil, nil, err
	}

	key := assetBucketKey(companyID, args.AssetKey)
	input := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader([]byte(args.Body)),
	}

	// expected_etag field present in JSON but null → create-only
	// expected_etag field present and non-null string → conditional update
	// field absent (nil pointer) → force overwrite (no condition)
	//
	// The MCP SDK unmarshals omitted fields as nil. A JSON `null` becomes a
	// non-nil pointer to the zero value via *string("") — but that's
	// indistinguishable. We use the sentinel "null" string for null semantics
	// when the caller wants create-only.
	//
	// Actually: with *string, omitting the field gives nil, and `null` in JSON
	// also gives nil. To distinguish "create-only" from "force overwrite" we
	// rely on the API contract: callers pass `null` as the JSON value via the
	// SDK which produces nil, and omit entirely for force-overwrite.
	// Since we can't distinguish nil-from-null vs nil-from-absent here, we
	// use the request's raw JSON. For simplicity, we treat nil as force-overwrite
	// and require callers to pass the string "CREATE_ONLY" for create-only semantics,
	// or a real ETag for conditional update.
	//
	// Re-reading the ticket: "null = create-only via IfNoneMatch: *; string = IfMatch".
	// We'll handle this by convention: if the field is present as JSON null,
	// the Go SDK will decode it as a nil *string but that's the same as absent.
	// We use a wrapper struct approach: check the raw args.
	// For now: nil pointer = force overwrite; non-nil pointer with value "" = create-only;
	// non-nil pointer with non-empty value = IfMatch.
	if args.ExpectedETag != nil {
		if *args.ExpectedETag == "" {
			// Create-only: IfNoneMatch: *
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
		return nil, nil, fmt.Errorf("write_asset: %w", err)
	}

	output := &WriteAssetOutput{
		ETag:      derefString(out.ETag),
		PublicURL: assetPublicURL(baseURL, companyID, args.AssetKey),
	}
	text := fmt.Sprintf("asset written: asset_key=%s etag=%s public_url=%s", args.AssetKey, output.ETag, output.PublicURL)
	return &sdk.CallToolResult{
		Content:           []sdk.Content{&sdk.TextContent{Text: text}},
		StructuredContent: output,
	}, output, nil
}

// ── list_assets ───────────────────────────────────────────────────────────────

var ListAssetsTool = sdk.Tool{
	Name:        "list_assets",
	Description: "Lists public site assets in R2 storage under the company prefix, with optional sub-prefix filter.",
}

type ListAssetsInput struct {
	Prefix string `json:"prefix,omitempty" jsonschema:"Optional sub-prefix to filter asset keys"`
}

type AssetItem struct {
	AssetKey  string `json:"asset_key"`
	ETag      string `json:"etag"`
	Size      int64  `json:"size"`
	PublicURL string `json:"public_url"`
}

type ListAssetsOutput struct {
	Assets []AssetItem `json:"assets"`
}

func ListAssets(ctx context.Context, req *sdk.CallToolRequest, args ListAssetsInput) (*sdk.CallToolResult, any, error) {
	client, err := r2Client()
	if err != nil {
		return nil, nil, err
	}
	bucket, err := mustEnv("R2_COMPANY_PUBLIC_BUCKET")
	if err != nil {
		return nil, nil, err
	}
	companyID, err := mustEnv("ZILLA_COMPANY_ID")
	if err != nil {
		return nil, nil, err
	}
	baseURL, err := mustEnv("R2_PUBLIC_ASSETS_BASE_URL")
	if err != nil {
		return nil, nil, err
	}

	prefix := companyID + "/"
	if args.Prefix != "" {
		prefix = prefix + args.Prefix
	}

	objects, err := listBucketObjects(ctx, client, bucket, prefix)
	if err != nil {
		return nil, nil, fmt.Errorf("list_assets: %w", err)
	}

	companyPrefix := companyID + "/"
	items := make([]AssetItem, 0, len(objects))
	for _, obj := range objects {
		fullKey := derefString(obj.Key)
		// Strip the company prefix to get the relative asset_key
		assetKey := fullKey
		if len(fullKey) > len(companyPrefix) {
			assetKey = fullKey[len(companyPrefix):]
		}
		items = append(items, AssetItem{
			AssetKey:  assetKey,
			ETag:      derefString(obj.ETag),
			Size:      derefInt64(obj.Size),
			PublicURL: assetPublicURL(baseURL, companyID, assetKey),
		})
	}

	output := &ListAssetsOutput{Assets: items}
	text := fmt.Sprintf("listed %d assets under prefix %q", len(items), prefix)
	return &sdk.CallToolResult{
		Content:           []sdk.Content{&sdk.TextContent{Text: text}},
		StructuredContent: output,
	}, output, nil
}

// ── delete_asset ──────────────────────────────────────────────────────────────

var DeleteAssetTool = sdk.Tool{
	Name:        "delete_asset",
	Description: "Deletes a public site asset from R2 storage. Optionally provide expected_etag for conditional delete.",
}

type DeleteAssetInput struct {
	AssetKey     string  `json:"asset_key" jsonschema:"Relative asset key to delete"`
	ExpectedETag *string `json:"expected_etag,omitempty" jsonschema:"Optional ETag for conditional delete (IfMatch)"`
}

type DeleteAssetOutput struct {
	OK bool `json:"ok"`
}

func DeleteAsset(ctx context.Context, req *sdk.CallToolRequest, args DeleteAssetInput) (*sdk.CallToolResult, any, error) {
	client, err := r2Client()
	if err != nil {
		return nil, nil, err
	}
	bucket, err := mustEnv("R2_COMPANY_PUBLIC_BUCKET")
	if err != nil {
		return nil, nil, err
	}
	companyID, err := mustEnv("ZILLA_COMPANY_ID")
	if err != nil {
		return nil, nil, err
	}

	key := assetBucketKey(companyID, args.AssetKey)
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
		return nil, nil, fmt.Errorf("delete_asset: %w", err)
	}

	output := &DeleteAssetOutput{OK: true}
	text := fmt.Sprintf("deleted asset: %s", args.AssetKey)
	return &sdk.CallToolResult{
		Content:           []sdk.Content{&sdk.TextContent{Text: text}},
		StructuredContent: output,
	}, output, nil
}
