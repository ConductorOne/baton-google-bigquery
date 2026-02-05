package connector

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/bigquery"
	resourcemanager "cloud.google.com/go/resourcemanager/apiv3"
	"cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	ent "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	grant "github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"google.golang.org/api/iterator"
)

const memberEntitlement = "member"

type projectBuilder struct {
	resourceType   *v2.ResourceType
	projectsClient *resourcemanager.ProjectsClient
	bigQueryClient *bigquery.Client
}

func (p *projectBuilder) ResourceType(ctx context.Context) *v2.ResourceType {
	return projectResourceType
}

func (p *projectBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, opts rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error) {
	var (
		resources []*v2.Resource
		bag       = &pagination.Bag{}
	)
	err := bag.Unmarshal(opts.PageToken.Token)
	if err != nil {
		return nil, nil, err
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
			if !isPermissionDenied(ctx, err) {
				return nil, nil, wrapError(err, "Unable to fetch projects")
			}
		}

		resource, err := projectResource(project)
		if err != nil {
			return nil, nil, wrapError(err, "Unable to create project resource")
		}

		resources = append(resources, resource)
	}

	err = bag.Next(it.PageInfo().Token)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to fetch bag.Next: %w", err)
	}

	pageToken, err := bag.Marshal()
	if err != nil {
		return nil, nil, err
	}

	return resources, &rs.SyncOpResults{NextPageToken: pageToken}, nil
}

func (p *projectBuilder) Entitlements(_ context.Context, resource *v2.Resource, _ rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	var rv []*v2.Entitlement
	assigmentOptions := []ent.EntitlementOption{
		ent.WithGrantableTo(userResourceType),
		ent.WithDescription(fmt.Sprintf("Member of %s project", resource.DisplayName)),
		ent.WithDisplayName(fmt.Sprintf("%s project %s", resource.DisplayName, memberEntitlement)),
	}
	rv = append(rv, ent.NewAssignmentEntitlement(resource, memberEntitlement, assigmentOptions...))

	return rv, &rs.SyncOpResults{}, nil
}

func (p *projectBuilder) Grants(ctx context.Context, resource *v2.Resource, opts rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	var rv []*v2.Grant
	iter := p.bigQueryClient.Datasets(ctx)
	iter.ProjectID = resource.Id.Resource // Setting ProjectID
	for {
		dataset, err := iter.Next()
		if errors.Is(err, iterator.Done) || dataset == nil {
			break
		}

		if err != nil {
			if !isPermissionDenied(ctx, err) {
				return nil, nil, wrapError(err, "Unable to fetch dataset")
			}
		}

		membershipGrant := grant.NewGrant(resource,
			memberEntitlement,
			&v2.ResourceId{
				ResourceType: datasetResourceType.Id,
				Resource:     dataset.DatasetID,
			})
		rv = append(rv, membershipGrant)
	}

	return rv, &rs.SyncOpResults{}, nil
}

func newProjectBuilder(projectsClient *resourcemanager.ProjectsClient, bigQueryClient *bigquery.Client) *projectBuilder {
	return &projectBuilder{
		resourceType:   projectResourceType,
		projectsClient: projectsClient,
		bigQueryClient: bigQueryClient,
	}
}
