package connector

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/bigquery"
	resourcemanager "cloud.google.com/go/resourcemanager/apiv3"
	"cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	ent "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"
)

type projectBuilder struct {
	resourceType      *v2.ResourceType
	projectsClient    *resourcemanager.ProjectsClient
	bigQueryClient    *bigquery.Client
	excludeProjectIDs []string
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
	var (
		resources []*v2.Resource
		projectId = p.bigQueryClient.Project()
	)
	l := ctxzap.Extract(ctx)
	if isExcluded(p.excludeProjectIDs, projectId) {
		l.Warn(
			"baton-google-bigquery: project in exclusion list",
			zap.String("projectId", projectId),
		)

		return resources, "", nil, nil
	}

	it := p.projectsClient.SearchProjects(ctx,
		&resourcemanagerpb.SearchProjectsRequest{
			Query: "",
		},
	)
	for {
		projects, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}

		if err != nil {
			return nil, "", nil, wrapError(err, "Unable to fetch ptoject")
		}

		resource, err := projectResource(projects)
		if err != nil {
			return nil, "", nil, wrapError(err, "Unable to create project resource")
		}

		resources = append(resources, resource)
	}

	return resources, "", nil, nil
}

func (p *projectBuilder) Entitlements(_ context.Context, resource *v2.Resource, _ *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	var rv []*v2.Entitlement
	assigmentOptions := []ent.EntitlementOption{
		ent.WithGrantableTo(userResourceType),
		ent.WithDescription(fmt.Sprintf("Assigned to %s project", resource.DisplayName)),
		ent.WithDisplayName(fmt.Sprintf("%s project %s", resource.DisplayName, assignedEntitlement)),
	}
	rv = append(rv, ent.NewAssignmentEntitlement(resource, assignedEntitlement, assigmentOptions...))

	assigmentOptions = []ent.EntitlementOption{
		ent.WithGrantableTo(serviceAccountResourceType),
		ent.WithDescription(fmt.Sprintf("Assigned to %s project", resource.DisplayName)),
		ent.WithDisplayName(fmt.Sprintf("%s project %s", resource.DisplayName, assignedEntitlement)),
	}
	rv = append(rv, ent.NewAssignmentEntitlement(resource, assignedEntitlement, assigmentOptions...))

	return rv, "", nil, nil
}

func (p *projectBuilder) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	var grants []*v2.Grant

	return grants, "", nil, nil
}

func newProjectBuilder(projectsClient *resourcemanager.ProjectsClient, bigQueryClient *bigquery.Client) *projectBuilder {
	return &projectBuilder{
		resourceType:   projectResourceType,
		projectsClient: projectsClient,
		bigQueryClient: bigQueryClient,
	}
}
