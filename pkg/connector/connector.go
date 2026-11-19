package connector

import (
	"context"
	"fmt"
	"io"
	"os"

	"cloud.google.com/go/bigquery"
	resourcemanager "cloud.google.com/go/resourcemanager/apiv3"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	googlecredentialguard "github.com/conductorone/google-credential-guard"
	"google.golang.org/api/option"
)

type GoogleBigQuery struct {
	ProjectsClient *resourcemanager.ProjectsClient
	BigQueryClient *bigquery.Client
}

// ResourceSyncers returns a ResourceSyncer for each resource type that should be synced from the upstream service.
func (d *GoogleBigQuery) ResourceSyncers(ctx context.Context) []connectorbuilder.ResourceSyncer {
	return []connectorbuilder.ResourceSyncer{
		newUserBuilder(d.ProjectsClient, d.BigQueryClient),
		newRoleBuilder(d.ProjectsClient, d.BigQueryClient),
		newDatasetBuilder(d.BigQueryClient, d.ProjectsClient),
		newProjectBuilder(d.ProjectsClient, d.BigQueryClient),
	}
}

// Asset takes an input AssetRef and attempts to fetch it using the connector's authenticated http client
// It streams a response, always starting with a metadata object, following by chunked payloads for the asset.
func (d *GoogleBigQuery) Asset(ctx context.Context, asset *v2.AssetRef) (string, io.ReadCloser, error) {
	return "", nil, nil
}

// Metadata returns metadata about the connector.
func (d *GoogleBigQuery) Metadata(ctx context.Context) (*v2.ConnectorMetadata, error) {
	return &v2.ConnectorMetadata{
		DisplayName: "My Baton Connector",
		Description: "The template implementation of a baton connector",
	}, nil
}

// Validate is called to ensure that the connector is properly configured. It should exercise any API credentials
// to be sure that they are valid.
func (d *GoogleBigQuery) Validate(ctx context.Context) (annotations.Annotations, error) {
	if projectId := d.BigQueryClient.Project(); projectId == "" {
		return nil, fmt.Errorf("project id is empty")
	}

	return nil, nil
}

// New returns a new instance of the connector.
func New(ctx context.Context, credentialsJSONFilePath string) (*GoogleBigQuery, error) {
	file, err := os.Open(credentialsJSONFilePath)
	if err != nil {
		return nil, fmt.Errorf("bigquery: open credentials file: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("bigquery: stat credentials file: %w", err)
	}
	if !info.Mode().IsRegular() || info.Size() > googlecredentialguard.MaxJSONBytes {
		return nil, fmt.Errorf("bigquery: credentials file must be regular and at most %d bytes", googlecredentialguard.MaxJSONBytes)
	}
	data, err := io.ReadAll(io.LimitReader(file, googlecredentialguard.MaxJSONBytes+1))
	if err != nil {
		return nil, fmt.Errorf("bigquery: read credentials file: %w", err)
	}
	return NewFromJSONBytes(ctx, data)
}

func NewFromJSONBytes(ctx context.Context, credentialsJSON []byte) (*GoogleBigQuery, error) {
	key, err := googlecredentialguard.ParseServiceAccountJSON(credentialsJSON)
	if err != nil {
		return nil, fmt.Errorf("bigquery: parse credentials JSON: %w", err)
	}
	return createClient(ctx, option.WithAuthCredentialsJSON(option.ServiceAccount, key.CanonicalJSON()))
}

func createClient(ctx context.Context, opts ...option.ClientOption) (*GoogleBigQuery, error) {
	projectsClient, err := resourcemanager.NewProjectsClient(ctx, opts...)
	if err != nil {
		return nil, err
	}

	bigQueryClient, err := bigquery.NewClient(ctx, bigquery.DetectProjectID, opts...)
	if err != nil {
		return nil, err
	}

	return &GoogleBigQuery{
		ProjectsClient: projectsClient,
		BigQueryClient: bigQueryClient,
	}, nil
}
