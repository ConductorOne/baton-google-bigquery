package connector

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/iam/apiv1/iampb"
	resourcemanager "cloud.google.com/go/resourcemanager/apiv3"
	"cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"
)

type userBuilder struct {
	resourceType      *v2.ResourceType
	ProjectsClient    *resourcemanager.ProjectsClient
	BigQueryClient    *bigquery.Client
	ProjectsWhitelist []string
}

func (o *userBuilder) ResourceType(ctx context.Context) *v2.ResourceType {
	return userResourceType
}

func userResource(member string, parentResourceID *v2.ResourceId) (*v2.Resource, error) {
	profile := map[string]interface{}{
		"email": member,
	}

	userTrairs := []rs.UserTraitOption{
		rs.WithUserProfile(profile),
		rs.WithUserLogin(member),
		rs.WithStatus(v2.UserTrait_Status_STATUS_ENABLED),
	}

	resource, err := rs.NewUserResource(member,
		userResourceType,
		member,
		userTrairs,
		rs.WithParentResourceID(parentResourceID),
	)
	if err != nil {
		return nil, err
	}

	return resource, nil
}

// List returns all the users from the database as resource objects.
// Users include a UserTrait because they are the 'shape' of a standard user.
func (o *userBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
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
			return nil, "", nil, wrapError(err, "Unable to fetch project")
		}

		if len(o.ProjectsWhitelist) > 0 && !isWhiteListed(o.ProjectsWhitelist, project.ProjectId) {
			l.Warn(
				"baton-google-bigquery: project is not whitelisted",
				zap.String("projectId", project.ProjectId),
			)

			return resources, "", nil, nil
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
				isUser, member := isUser(member)
				if !isUser {
					continue
				}

				resource, err := userResource(member, &v2.ResourceId{
					ResourceType: projectResourceType.Id,
					Resource:     project.ProjectId,
				})
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

func isUser(member string) (bool, string) {
	if strings.HasPrefix(member, "user:") {
		return true, strings.TrimPrefix(member, "user:")
	}

	return false, ""
}

// Entitlements always returns an empty slice for users.
func (o *userBuilder) Entitlements(_ context.Context, resource *v2.Resource, _ *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

// Grants always returns an empty slice for users since they don't have any entitlements.
func (o *userBuilder) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func newUserBuilder(projectsClient *resourcemanager.ProjectsClient,
	bigQueryClient *bigquery.Client,
	projectsWhitelist []string,
) *userBuilder {
	return &userBuilder{
		resourceType:      userResourceType,
		ProjectsClient:    projectsClient,
		BigQueryClient:    bigQueryClient,
		ProjectsWhitelist: projectsWhitelist,
	}
}
