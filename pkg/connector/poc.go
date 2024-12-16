package connector

import (
	"context"
	"errors"
	"log"

	"cloud.google.com/go/bigquery"
	resourcemanager "cloud.google.com/go/resourcemanager/apiv3"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	cloudresourcemanager "google.golang.org/api/cloudresourcemanager/v1"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type pocBuilder struct {
	resourceType      *v2.ResourceType
	ProjectsClient    *resourcemanager.ProjectsClient
	BigQueryClient    *bigquery.Client
	ProjectsWhitelist []string
	Opts              []option.ClientOption
}

func (po *pocBuilder) ResourceType(ctx context.Context) *v2.ResourceType {
	return pocResourceType
}

// Iterate over all projects in the organization and retrieves their datasets.
func (po *pocBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	var (
		resources []*v2.Resource
		bag       = &pagination.Bag{}
	)

	err := bag.Unmarshal(pToken.Token)
	if err != nil {
		return nil, "", nil, err
	}

	if bag.Current() == nil {
		bag.Push(pagination.PageState{
			ResourceTypeID: projectResourceType.Id,
		})
	}

	// Create a Cloud Resource Manager client to list projects in the org.
	// Ensure you enable the cloud resource manager api.
	crmService, err := cloudresourcemanager.NewService(ctx, po.Opts...)
	if err != nil {
		return nil, "", nil, err
	}

	// List all projects in the organization
	projectListCall := crmService.Projects.List()
	projects, err := projectListCall.Do()
	if err != nil {
		return nil, "", nil, err
	}

	// For each project, list datasets in BigQuery
	// It lists all datasets using the Datasets iterator.
	// Ensure the account has resourcemanager.projects.list permission
	// and access to list datasets in BigQuery for each project.
	for _, project := range projects.Projects {
		log.Printf("Project ID: %s\n", project.ProjectId)
		bqClient, err := bigquery.NewClient(ctx, project.ProjectId, po.Opts...)
		if err != nil {
			continue
		}

		it := bqClient.Datasets(ctx)
		for {
			dataset, err := it.Next()
			if errors.Is(err, iterator.Done) || dataset == nil {
				break
			}

			if err != nil {
				break
			}
			log.Printf("  Dataset ID: %s\n", dataset.DatasetID)
			resource, err := pocResource(ctx, dataset.DatasetID, &v2.ResourceId{
				ResourceType: projectResourceType.Id,
				Resource:     dataset.ProjectID,
			})
			if err != nil {
				return nil, "", nil, wrapError(err, "Unable to create dataset resource")
			}

			resources = append(resources, resource)
		}
		bqClient.Close()
	}

	return resources, "", nil, nil
}

// Entitlements always returns an empty slice for users.
func (po *pocBuilder) Entitlements(_ context.Context, resource *v2.Resource, _ *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

// Grants always returns an empty slice for users since they don't have any entitlements.
func (o *pocBuilder) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func newPocBuilder(projectsClient *resourcemanager.ProjectsClient,
	bigQueryClient *bigquery.Client,
	projectsWhitelist []string,
	opts []option.ClientOption,
) *pocBuilder {
	return &pocBuilder{
		resourceType:      pocResourceType,
		ProjectsClient:    projectsClient,
		BigQueryClient:    bigQueryClient,
		ProjectsWhitelist: projectsWhitelist,
		Opts:              opts,
	}
}
