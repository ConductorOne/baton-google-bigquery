package connector

import (
	"context"
	"fmt"
	"io"

	"cloud.google.com/go/bigquery"
	resourcemanager "cloud.google.com/go/resourcemanager/apiv3"
	"github.com/conductorone/baton-google-bigquery/pkg/config"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/cli"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
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
	opt := option.WithCredentialsFile(credentialsJSONFilePath)

	return createClient(ctx, opt)
}

func NewFromJSONBytes(ctx context.Context, credentialsJSON []byte) (*GoogleBigQuery, error) {
	opt := option.WithCredentialsJSON(credentialsJSON)

	return createClient(ctx, opt)
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

// NewConnector returns a new connector builder from a configuration struct.
func NewConnector(ctx context.Context, cfg *config.GoogleBigQuery, opts *cli.ConnectorOpts) (connectorbuilder.ConnectorBuilderV2, []connectorbuilder.Opt, error) {
	l := ctxzap.Extract(ctx)

	cb, err := New(ctx, cfg.CredentialsJSONFilePath)
	if err != nil {
		l.Error("error creating connector", zap.Error(err))
		return nil, nil, err
	}

	return cb, nil, nil
}
