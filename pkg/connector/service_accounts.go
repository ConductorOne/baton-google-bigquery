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

type serviceAccountBuilder struct {
	resourceType   *v2.ResourceType
	ProjectsClient *resourcemanager.ProjectsClient
	BigQueryClient *bigquery.Client
}

func (o *serviceAccountBuilder) ResourceType(ctx context.Context) *v2.ResourceType {
	return serviceAccountResourceType
}

func serviceAccountResource(member string) (*v2.Resource, error) {
	profile := map[string]interface{}{
		"email": member,
	}

	serviceAccountTraits := []rs.AppTraitOption{
		rs.WithAppProfile(profile),
	}

	resource, err := rs.NewAppResource(member, serviceAccountResourceType, member, serviceAccountTraits)
	if err != nil {
		return nil, err
	}

	return resource, nil
}

// List returns all the users from the database as resource objects.
// Users include a UserTrait because they are the 'shape' of a standard user.
func (o *serviceAccountBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	policy, err := o.ProjectsClient.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{
		Resource: fmt.Sprintf("projects/%s", o.BigQueryClient.Project()),
	})
	if err != nil {
		return nil, "", nil, wrapError(err, "failed to get IAM policy")
	}

	var resources []*v2.Resource
	for _, binding := range policy.Bindings {
		for _, member := range binding.Members {
			isServiceAccount, member := isServiceAccount(member)
			if !isServiceAccount {
				continue
			}

			resource, err := serviceAccountResource(member)
			if err != nil {
				return nil, "", nil, wrapError(err, "failed to create service account resource")
			}

			resources = append(resources, resource)
		}
	}

	return resources, "", nil, nil
}

func isServiceAccount(member string) (bool, string) {
	if strings.HasPrefix(member, "serviceAccount:") {
		return true, strings.TrimPrefix(member, "serviceAccount:")
	}

	return false, ""
}

// Entitlements always returns an empty slice for users.
func (o *serviceAccountBuilder) Entitlements(_ context.Context, resource *v2.Resource, _ *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

// Grants always returns an empty slice for users since they don't have any entitlements.
func (o *serviceAccountBuilder) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func newServiceAccountBuilder(projectsClient *resourcemanager.ProjectsClient, bigQueryClient *bigquery.Client) *serviceAccountBuilder {
	return &serviceAccountBuilder{
		resourceType:   serviceAccountResourceType,
		ProjectsClient: projectsClient,
		BigQueryClient: bigQueryClient,
	}
}
