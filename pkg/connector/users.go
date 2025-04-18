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
	sdkResource "github.com/conductorone/baton-sdk/pkg/types/resource"
	"google.golang.org/api/iterator"
)

type userBuilder struct {
	resourceType   *v2.ResourceType
	ProjectsClient *resourcemanager.ProjectsClient
	BigQueryClient *bigquery.Client
}

func (o *userBuilder) ResourceType(ctx context.Context) *v2.ResourceType {
	return userResourceType
}

// List returns all the users from the database as resource objects.
// Users include a UserTrait because they are the 'shape' of a standard user.
func (o *userBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
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

	it := o.ProjectsClient.SearchProjects(ctx,
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

		policy, err := o.ProjectsClient.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{
			Resource: fmt.Sprintf("projects/%s", project.ProjectId),
		})
		if err != nil {
			if !isPermissionDenied(ctx, err) {
				return nil, "", nil, wrapError(err, "listing users failed")
			}
		}

		if policy == nil {
			return resources, "", nil, nil
		}

		for _, binding := range policy.Bindings {
			for _, member := range binding.Members {
				var userString string
				var accountTrait sdkResource.UserTraitOption = nil
				if isUserBool, _ := isUser(member); isUserBool {
					_, userString = isUser(member)
				} else if isServiceAccountBool, _ := isServiceAccount(member); isServiceAccountBool {
					_, userString = isServiceAccount(member)
					accountTrait = sdkResource.WithAccountType(v2.UserTrait_ACCOUNT_TYPE_SERVICE)
				} else {
					continue
				}
				var resource *v2.Resource
				var err error
				resource, err = userResource(userString, &v2.ResourceId{
					ResourceType: projectResourceType.Id,
					Resource:     project.ProjectId,
				}, accountTrait)
				if err != nil {
					return nil, "", nil, wrapError(err, "failed to create user resource")
				}

				resources = append(resources, resource)
			}
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

// Entitlements always returns an empty slice for users.
func (o *userBuilder) Entitlements(_ context.Context, resource *v2.Resource, _ *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

// Grants always returns an empty slice for users since they don't have any entitlements.
func (o *userBuilder) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func newUserBuilder(projectsClient *resourcemanager.ProjectsClient, bigQueryClient *bigquery.Client) *userBuilder {
	return &userBuilder{
		resourceType:   userResourceType,
		ProjectsClient: projectsClient,
		BigQueryClient: bigQueryClient,
	}
}
