package connector

import (
	"context"

	"cloud.google.com/go/bigquery"
	resourcemanager "cloud.google.com/go/resourcemanager/apiv3"
	"cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

type projectBuilder struct {
	resourceType   *v2.ResourceType
	projectsClient *resourcemanager.ProjectsClient
	bigQueryClient *bigquery.Client
}

func (p *projectBuilder) ResourceType(ctx context.Context) *v2.ResourceType {
	return projectResourceType
}

func projectResource(projects *resourcemanagerpb.Project) (*v2.Resource, error) {
	var opts []rs.ResourceOption
	profile := map[string]interface{}{
		"id":          projects.ProjectId,
		"name":        projects.Name,
		"displayName": projects.DisplayName,
	}

	projectTraitOptions := []rs.AppTraitOption{
		rs.WithAppProfile(profile),
	}

	opts = append(opts, rs.WithAppTrait(projectTraitOptions...))
	resource, err := rs.NewResource(
		projects.DisplayName,
		projectResourceType,
		projects.ProjectId,
		opts...,
	)
	if err != nil {
		return nil, err
	}

	return resource, nil
}

func (p *projectBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	var resources []*v2.Resource
	bag := &pagination.Bag{}
	it := p.projectsClient.SearchProjects(ctx,
		&resourcemanagerpb.SearchProjectsRequest{
			Query: "",
		},
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

	project, err := it.Next()
	if err != nil {
		return nil, "", nil, err
	}

	resource, err := projectResource(project)
	if err != nil {
		return nil, "", nil, wrapError(err, "Unable to create project resource")
	}

	resources = append(resources, resource)
	bag.Pop()
	pageToken, err := bag.Marshal()
	if err != nil {
		return nil, "", nil, err
	}

	return resources, pageToken, nil, nil
}

func (p *projectBuilder) Entitlements(_ context.Context, resource *v2.Resource, _ *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func (p *projectBuilder) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func newProjectBuilder(projectsClient *resourcemanager.ProjectsClient, bigQueryClient *bigquery.Client) *projectBuilder {
	return &projectBuilder{
		resourceType:   projectResourceType,
		projectsClient: projectsClient,
		bigQueryClient: bigQueryClient,
	}
}
