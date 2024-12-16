package connector

import (
	"context"
	"errors"
	"fmt"
	"log"

	"cloud.google.com/go/bigquery"
	resourcemanager "cloud.google.com/go/resourcemanager/apiv3"
	"cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	cloudresourcemanager "google.golang.org/api/cloudresourcemanager/v1"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type pocBuilder struct {
	resourceType   *v2.ResourceType
	ProjectsClient *resourcemanager.ProjectsClient
	BigQueryClient *bigquery.Client
	Opts           []option.ClientOption
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

	projectsR, err := listProjects(ctx, po.Opts...)
	if err != nil {
		log.Fatalf("Failed to list projects: %v", err)
	}
	log.Println(projectsR)

	err = bag.Unmarshal(pToken.Token)
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
	opts []option.ClientOption,
) *pocBuilder {
	return &pocBuilder{
		resourceType:   pocResourceType,
		ProjectsClient: projectsClient,
		BigQueryClient: bigQueryClient,
		Opts:           opts,
	}
}

func listProjects(ctx context.Context, opts ...option.ClientOption) ([]string, error) {
	activeProjects := []string{}
	folders, err := getFolders(ctx, "organizations/666599870419", nil, opts...)
	if err != nil {
		return nil, err
	}

	for _, folder := range folders {
		projects, err := searchProjects(ctx, folder)
		if err != nil {
			return nil, err
		}

		for _, project := range projects {
			if project.State.String() == "ACTIVE" {
				activeProjects = append(activeProjects, project.ProjectId)
			}
		}
	}
	return activeProjects, nil
}

func searchProjects(ctx context.Context, folderID string) ([]*resourcemanagerpb.Project, error) {
	client, err := resourcemanager.NewProjectsClient(ctx)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	query := fmt.Sprintf("parent:%s", folderID)
	req := &resourcemanagerpb.SearchProjectsRequest{
		Query: query,
	}

	it := client.SearchProjects(ctx, req)
	var searchResult []*resourcemanagerpb.Project
	for {
		project, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		searchResult = append(searchResult, project)
	}
	return searchResult, nil
}

func getFolders(ctx context.Context, parentID string, folders []string, opts ...option.ClientOption) ([]string, error) {
	if folders == nil {
		folders = []string{}
	}

	client, err := resourcemanager.NewFoldersClient(ctx, opts...)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	req := &resourcemanagerpb.ListFoldersRequest{
		Parent: parentID,
	}

	it := client.ListFolders(ctx, req)
	for {
		folder, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		folders = append(folders, folder.Name)
		folders, err = getFolders(ctx, folder.Name, folders)
		if err != nil {
			return nil, err
		}
	}
	return folders, nil
}
