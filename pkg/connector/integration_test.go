package connector

import (
	"context"
	"testing"

	"cloud.google.com/go/bigquery"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/stretchr/testify/require"
)

var (
	// jsonFilePath = os.Get env("BATON_CREDENTIALS_JSON_FILE_PATH")
	ctxTest      = context.Background()
	jsonFilePath = "../../c2.json"
	// jsonFilePath = "../../insulator-gcp-cred.json"
)

func TestDatasetBuilderList(t *testing.T) {
	if jsonFilePath == "" {
		t.Skip()
	}

	cliTest, err := getClientForTesting(ctxTest)
	require.Nil(t, err)

	o := &datasetBuilder{
		resourceType:   datasetResourceType,
		bigQueryClient: cliTest.BigQueryClient,
		projectsClient: cliTest.ProjectsClient,
	}

	_, _, _, err = o.List(ctxTest, &v2.ResourceId{}, &pagination.Token{})
	require.Nil(t, err)
}

func getClientForTesting(ctx context.Context) (*GoogleBigQuery, error) {
	return New(ctx, jsonFilePath)
}

func TestUserBuilderList(t *testing.T) {
	if jsonFilePath == "" {
		t.Skip()
	}

	cliTest, err := getClientForTesting(ctxTest)
	require.Nil(t, err)

	u := &userBuilder{
		resourceType:   datasetResourceType,
		BigQueryClient: cliTest.BigQueryClient,
		ProjectsClient: cliTest.ProjectsClient,
	}

	_, _, _, err = u.List(ctxTest, &v2.ResourceId{}, &pagination.Token{})
	require.Nil(t, err)
}

func TestCreateDatasets(t *testing.T) {
	if jsonFilePath == "" {
		t.Skip()
	}

	cliTest, err := getClientForTesting(ctxTest)
	require.Nil(t, err)

	meta := &bigquery.DatasetMetadata{
		Location: "US", // See https://cloud.google.com/bigquery/docs/locations
	}

	err = cliTest.BigQueryClient.Dataset("localdataset").Create(ctxTest, meta)
	require.Nil(t, err)
}