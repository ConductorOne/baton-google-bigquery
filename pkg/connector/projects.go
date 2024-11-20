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
	grant "github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"
)

const memberEntitlement = "member"

type projectBuilder struct {
	resourceType      *v2.ResourceType
	projectsClient    *resourcemanager.ProjectsClient
	bigQueryClient    *bigquery.Client
	ProjectsWhitelist []string
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
		bag       = &pagination.Bag{}
	)
	l := ctxzap.Extract(ctx)
	err := bag.Unmarshal(pToken.Token)
	if err != nil {
		return nil, "", nil, err
	}

	if bag.Current() == nil {
		bag.Push(pagination.PageState{
			ResourceTypeID: projectResourceType.Id,
		})
	}

	it := p.projectsClient.SearchProjects(ctx,
		&resourcemanagerpb.SearchProjectsRequest{
			Query:     "",
			PageToken: bag.PageToken(),
		},
	)

	for {
		project, err := it.Next()
		if errors.Is(err, iterator.Done) || project == nil {
			break
		}

		if err != nil {
			return nil, "", nil, wrapError(err, "Unable to fetch projects")
		}

		if len(p.ProjectsWhitelist) > 0 && !isWhiteListed(p.ProjectsWhitelist, project.ProjectId) {
			l.Warn(
				"baton-google-bigquery: project is not whitelisted",
				zap.String("projectId", project.ProjectId),
			)

			return resources, "", nil, nil
		}

		resource, err := projectResource(project)
		if err != nil {
			return nil, "", nil, wrapError(err, "Unable to create project resource")
		}

		resources = append(resources, resource)
	}

	err = bag.Next(it.PageInfo().Token)
	if err != nil {
		return nil, "", nil, fmt.Errorf("failed to fetch bag.Next: %w", err)
	}

	pageToken, err := bag.Marshal()
	if err != nil {
		return nil, "", nil, err
	}

	return resources, pageToken, nil, nil
}

func (p *projectBuilder) Entitlements(_ context.Context, resource *v2.Resource, _ *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	var rv []*v2.Entitlement
	assigmentOptions := []ent.EntitlementOption{
		ent.WithGrantableTo(userResourceType),
		ent.WithDescription(fmt.Sprintf("Member of %s project", resource.DisplayName)),
		ent.WithDisplayName(fmt.Sprintf("%s project %s", resource.DisplayName, memberEntitlement)),
	}
	rv = append(rv, ent.NewAssignmentEntitlement(resource, memberEntitlement, assigmentOptions...))

	assigmentOptions = []ent.EntitlementOption{
		ent.WithGrantableTo(serviceAccountResourceType),
		ent.WithDescription(fmt.Sprintf("Assigned of %s project", resource.DisplayName)),
		ent.WithDisplayName(fmt.Sprintf("%s project %s", resource.DisplayName, memberEntitlement)),
	}
	rv = append(rv, ent.NewAssignmentEntitlement(resource, memberEntitlement, assigmentOptions...))

	return rv, "", nil, nil
}

func (p *projectBuilder) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	var rv []*v2.Grant
	l := ctxzap.Extract(ctx)
	iter := p.bigQueryClient.Datasets(ctx)
	iter.ProjectID = resource.Id.Resource // Setting ProjectID
	if len(p.ProjectsWhitelist) > 0 && !isWhiteListed(p.ProjectsWhitelist, iter.ProjectID) {
		l.Warn(
			"baton-google-bigquery: project is not whitelisted",
			zap.String("projectId", iter.ProjectID),
		)

		return rv, "", nil, nil
	}

	for {
		dataset, err := iter.Next()
		if errors.Is(err, iterator.Done) || dataset == nil {
			break
		}

		if err != nil {
			return nil, "", nil, wrapError(err, "Unable to fetch dataset")
		}

		principal := &v2.ResourceId{
			ResourceType: datasetResourceType.Id,
			Resource:     dataset.DatasetID,
		}
		membershipGrant := grant.NewGrant(resource, memberEntitlement, principal)
		rv = append(rv, membershipGrant)
	}

	return rv, "", nil, nil
}

func newProjectBuilder(projectsClient *resourcemanager.ProjectsClient,
	bigQueryClient *bigquery.Client,
	projectsWhitelist []string,
) *projectBuilder {
	return &projectBuilder{
		resourceType:      projectResourceType,
		projectsClient:    projectsClient,
		bigQueryClient:    bigQueryClient,
		ProjectsWhitelist: projectsWhitelist,
	}
}
