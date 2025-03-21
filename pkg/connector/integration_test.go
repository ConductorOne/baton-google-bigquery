package connector

import (
	"context"
	"os"
	"testing"

	"cloud.google.com/go/bigquery"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/stretchr/testify/require"
)

var (
	jsonFilePath = os.Getenv("BATON_CREDENTIALS/_JSON_FILE_PATH")
	ctxTest      = context.Background()
)

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
		resourceType:   userResourceType,
		BigQueryClient: cliTest.BigQueryClient,
		ProjectsClient: cliTest.ProjectsClient,
	}

	_, _, _, err = u.List(ctxTest, &v2.ResourceId{}, &pagination.Token{})
	require.Nil(t, err)
}

func TestProjectBuilderList(t *testing.T) {
	if jsonFilePath == "" {
		t.Skip()
	}

	cliTest, err := getClientForTesting(ctxTest)
	require.Nil(t, err)

	p := &projectBuilder{
		resourceType:   projectResourceType,
		bigQueryClient: cliTest.BigQueryClient,
		projectsClient: cliTest.ProjectsClient,
	}

	var token = "{}"
	for token != "" {
		_, pageToken, _, err := p.List(ctxTest, &v2.ResourceId{}, &pagination.Token{
			Token: token,
		})
		require.Nil(t, err)
		token = pageToken
	}
}

func TestCreateDataset(t *testing.T) {
	if jsonFilePath == "" {
		t.Skip()
	}

	cliTest, err := getClientForTesting(ctxTest)
	require.Nil(t, err)

	meta := &bigquery.DatasetMetadata{
		Location: "US", // See https://cloud.google.com/bigquery/docs/locations
	}

	// it uses the default project id within the creds.json
	err = cliTest.BigQueryClient.Dataset("localdataset_eu").Create(ctxTest, meta)
	require.Nil(t, err)
}

func TestDeleteDataset(t *testing.T) {
	if jsonFilePath == "" {
		t.Skip()
	}

	cliTest, err := getClientForTesting(ctxTest)
	require.Nil(t, err)

	// Delete the dataset. Delete will fail if the dataset is not empty.
	err = cliTest.BigQueryClient.Dataset("localdataset_eu").Delete(ctxTest)
	require.Nil(t, err)
}

func TestRoleBuilderList(t *testing.T) {
	if jsonFilePath == "" {
		t.Skip()
	}

	cliTest, err := getClientForTesting(ctxTest)
	require.Nil(t, err)

	u := &roleBuilder{
		resourceType:   datasetResourceType,
		bigQueryClient: cliTest.BigQueryClient,
		projectsClient: cliTest.ProjectsClient,
	}

	_, _, _, err = u.List(ctxTest, &v2.ResourceId{}, &pagination.Token{})
	require.Nil(t, err)
}

// To get the permission that you need to list datasets or get information on datasets,
// you need the BigQuery Metadata Viewer (roles/bigquery.metadataViewer) IAM role on your project.
// https://cloud.google.com/bigquery/docs/listing-datasets
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

func TestDatasetGrants(t *testing.T) {
	var (
		datasetID = "central_ds"
		projectId = "central-binder-441521-i4"
	)
	if jsonFilePath == "" {
		t.Skip()
	}

	cliTest, err := getClientForTesting(ctxTest)
	require.Nil(t, err)

	d := &datasetBuilder{
		resourceType:   datasetResourceType,
		bigQueryClient: cliTest.BigQueryClient,
		projectsClient: cliTest.ProjectsClient,
	}

	_, _, _, err = d.Grants(ctxTest, &v2.Resource{
		Id:               &v2.ResourceId{ResourceType: datasetResourceType.Id, Resource: datasetID},
		ParentResourceId: &v2.ResourceId{ResourceType: projectResourceType.Id, Resource: projectId},
	}, &pagination.Token{})
	require.Nil(t, err)
}
