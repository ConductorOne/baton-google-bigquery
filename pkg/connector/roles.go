package connector

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/iam/apiv1/iampb"
	resourcemanager "cloud.google.com/go/resourcemanager/apiv3"
	"cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	ent "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	grant "github.com/conductorone/baton-sdk/pkg/types/grant"
	"google.golang.org/api/iterator"
)

type roleBuilder struct {
	resourceType   *v2.ResourceType
	projectsClient *resourcemanager.ProjectsClient
	bigQueryClient *bigquery.Client
}

const assignedEntitlement = "assigned"

func (r *roleBuilder) ResourceType(ctx context.Context) *v2.ResourceType {
	return roleResourceType
}

func (r *roleBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
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

	it := r.projectsClient.SearchProjects(ctx,
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
				return nil, "", nil, wrapError(err, "Unable to fetch project")
			}
		}

		policy, err := r.projectsClient.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{
			Resource: fmt.Sprintf("projects/%s", project.ProjectId),
		})
		if err != nil {
			if !isPermissionDenied(ctx, err) {
				return nil, "", nil, wrapError(err, "failed to get IAM policy")
			}
		}

		if policy == nil {
			return resources, "", nil, nil
		}

		for _, binding := range policy.Bindings {
			resource, err := roleResource(binding.Role, &v2.ResourceId{
				ResourceType: projectResourceType.Id,
				Resource:     project.ProjectId,
			})
			if err != nil {
				return nil, "", nil, wrapError(err, "failed to create role resource")
			}

			resources = append(resources, resource)
		}
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

func (o *roleBuilder) Entitlements(_ context.Context, resource *v2.Resource, _ *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	var rv []*v2.Entitlement

	assigmentOptions := []ent.EntitlementOption{
		ent.WithGrantableTo(userResourceType),
		ent.WithDescription(fmt.Sprintf("Assigned to %s role", resource.DisplayName)),
		ent.WithDisplayName(fmt.Sprintf("%s role %s", resource.DisplayName, assignedEntitlement)),
	}
	rv = append(rv, ent.NewAssignmentEntitlement(resource, assignedEntitlement, assigmentOptions...))

	return rv, "", nil, nil
}

func (o *roleBuilder) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	var grants []*v2.Grant
	projectId := resource.ParentResourceId.Resource
	policy, err := o.projectsClient.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{
		Resource: fmt.Sprintf("projects/%s", projectId),
	})
	if err != nil {
		if !isPermissionDenied(ctx, err) {
			return nil, "", nil, wrapError(err, "listing grants for roles failed")
		}
	}

	if policy == nil {
		return grants, "", nil, nil
	}

	for _, binding := range policy.Bindings {
		if binding.Role != resource.Id.Resource {
			continue
		}

		for _, member := range binding.Members {
			if isUser, user := isUserOrServiceAccountMember(member); isUser {
				userResource, err := userResource(user, nil, nil)
				if err != nil {
					return nil, "", nil, wrapError(err, "failed to create user resource")
				}

				grants = append(grants, grant.NewGrant(resource, assignedEntitlement, userResource.Id))
			}
		}
	}

	return grants, "", nil, nil
}

func newRoleBuilder(projectsClient *resourcemanager.ProjectsClient, bigQueryClient *bigquery.Client) *roleBuilder {
	return &roleBuilder{
		resourceType:   roleResourceType,
		projectsClient: projectsClient,
		bigQueryClient: bigQueryClient,
	}
}
