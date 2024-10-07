package connector

import (
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/iam/apiv1/iampb"
	resourcemanager "cloud.google.com/go/resourcemanager/apiv3"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

type userBuilder struct {
	resourceType   *v2.ResourceType
	ProjectsClient *resourcemanager.ProjectsClient
	BigQueryClient *bigquery.Client
}

func (o *userBuilder) ResourceType(ctx context.Context) *v2.ResourceType {
	return userResourceType
}

func userResource(member string) (*v2.Resource, error) {
	profile := map[string]interface{}{
		"email": member,
	}

	userTrairs := []rs.UserTraitOption{
		rs.WithUserProfile(profile),
		rs.WithUserLogin(member),
		rs.WithStatus(v2.UserTrait_Status_STATUS_ENABLED),
	}

	resource, err := rs.NewUserResource(member, userResourceType, member, userTrairs)
	if err != nil {
		return nil, err
	}

	return resource, nil
}

// List returns all the users from the database as resource objects.
// Users include a UserTrait because they are the 'shape' of a standard user.
func (o *userBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	var resources []*v2.Resource
	policy, err := o.ProjectsClient.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{
		Resource: fmt.Sprintf("projects/%s", o.BigQueryClient.Project()),
	})
	if err != nil {
		if !isPermissionDenied(ctx, err) {
			return nil, "", nil, wrapError(err, "listing users failed")
		}
	}

	if policy != nil {
		for _, binding := range policy.Bindings {
			for _, member := range binding.Members {
				isUser, member := isUser(member)
				if !isUser {
					continue
				}

				resource, err := userResource(member)
				if err != nil {
					return nil, "", nil, wrapError(err, "failed to create user resource")
				}

				resources = append(resources, resource)
			}
		}
	}

	return resources, "", nil, nil
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

func newUserBuilder(projectsClient *resourcemanager.ProjectsClient, bigQueryClient *bigquery.Client) *userBuilder {
	return &userBuilder{
		resourceType:   userResourceType,
		ProjectsClient: projectsClient,
		BigQueryClient: bigQueryClient,
	}
}
