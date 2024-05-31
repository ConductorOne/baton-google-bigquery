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
	ent "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	grant "github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

type roleBuilder struct {
	resourceType   *v2.ResourceType
	projectsClient *resourcemanager.ProjectsClient
	bigQueryClient *bigquery.Client
}

const assignedEntitlement = "assigned"

func (o *roleBuilder) ResourceType(ctx context.Context) *v2.ResourceType {
	return roleResourceType
}

func roleResource(role string) (*v2.Resource, error) {
	roleName := removeRolesPrefix(role)

	profile := map[string]interface{}{
		"name": roleName,
	}

	roleTraitOptions := []rs.RoleTraitOption{
		rs.WithRoleProfile(profile),
	}

	resource, err := rs.NewRoleResource(roleName, roleResourceType, role, roleTraitOptions)
	if err != nil {
		return nil, err
	}

	return resource, nil
}

func removeRolesPrefix(role string) string {
	return strings.TrimPrefix(role, "roles/")
}

func (o *roleBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	policy, err := o.projectsClient.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{
		Resource: fmt.Sprintf("projects/%s", o.bigQueryClient.Project()),
	})
	if err != nil {
		return nil, "", nil, wrapError(err, "failed to get IAM policy")
	}

	var resources []*v2.Resource
	for _, binding := range policy.Bindings {
		resource, err := roleResource(binding.Role)
		if err != nil {
			return nil, "", nil, wrapError(err, "failed to create role resource")
		}

		resources = append(resources, resource)
	}

	return resources, "", nil, nil
}

func (o *roleBuilder) Entitlements(_ context.Context, resource *v2.Resource, _ *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	var rv []*v2.Entitlement

	assigmentOptions := []ent.EntitlementOption{
		ent.WithGrantableTo(userResourceType),
		ent.WithDescription(fmt.Sprintf("Assigned to %s role", resource.DisplayName)),
		ent.WithDisplayName(fmt.Sprintf("%s role %s", resource.DisplayName, assignedEntitlement)),
	}
	rv = append(rv, ent.NewAssignmentEntitlement(resource, assignedEntitlement, assigmentOptions...))

	assigmentOptions = []ent.EntitlementOption{
		ent.WithGrantableTo(serviceAccountResourceType),
		ent.WithDescription(fmt.Sprintf("Assigned to %s role", resource.DisplayName)),
		ent.WithDisplayName(fmt.Sprintf("%s role %s", resource.DisplayName, assignedEntitlement)),
	}
	rv = append(rv, ent.NewAssignmentEntitlement(resource, assignedEntitlement, assigmentOptions...))

	return rv, "", nil, nil
}

func (o *roleBuilder) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	policy, err := o.projectsClient.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{
		Resource: fmt.Sprintf("projects/%s", o.bigQueryClient.Project()),
	})
	if err != nil {
		return nil, "", nil, wrapError(err, "failed to get IAM policy")
	}

	var grants []*v2.Grant
	for _, binding := range policy.Bindings {
		if binding.Role != resource.Id.Resource {
			continue
		}

		for _, member := range binding.Members {
			// TODO: handle group bindings
			if isUser, user := isUser(member); isUser {
				userResource, err := userResource(user)
				if err != nil {
					return nil, "", nil, wrapError(err, "failed to create user resource")
				}

				grants = append(grants, grant.NewGrant(resource, assignedEntitlement, userResource.Id))
			} else if isServiceAccount, serviceAccount := isServiceAccount(member); isServiceAccount {
				serviceAccountResource, err := serviceAccountResource(serviceAccount)
				if err != nil {
					return nil, "", nil, wrapError(err, "failed to create service account resource")
				}

				grants = append(grants, grant.NewGrant(resource, assignedEntitlement, serviceAccountResource.Id))
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
