package connector

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/iam/apiv1/iampb"
	resourcemanager "cloud.google.com/go/resourcemanager/apiv3"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	ent "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	grant "github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"google.golang.org/api/iterator"
)

type datasetBuilder struct {
	resourceType   *v2.ResourceType
	bigQueryClient *bigquery.Client
	projectsClient *resourcemanager.ProjectsClient
}

const (
	ownerEntitlement  = "owner"
	writerEntitlement = "writer"
	viewerEntitlement = "viewer"
	accessEntitlement = "access"
)

var (
	/*
		API returns legacy roles for the dataset access. We need to map them to the new roles.
		See: https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets
		An IAM role ID that should be granted to the user, group, or domain specified in this access entry.
		The following legacy mappings will be applied:
		OWNER <=> roles/bigquery.dataOwner
		WRITER <=> roles/bigquery.dataEditor
		READER <=> roles/bigquery.dataViewer
		This field will accept any of the above formats, but will return only the legacy format. For example,
		if you set this field to "roles/bigquery.dataOwner", it will be returned back as "OWNER".
	*/
	ownerRole      = string(bigquery.OwnerRole)
	readerRole     = string(bigquery.ReaderRole)
	writerRole     = string(bigquery.WriterRole)
	legacyRolesMap = map[string]string{
		ownerRole:  "roles/bigquery.dataOwner", // TODO: remove roles/ prefix
		readerRole: "roles/bigquery.dataEditor",
		writerRole: "roles/bigquery.dataViewer",
	}
	legacyRolesToEntitlementsMap = map[string]string{
		ownerRole:  ownerEntitlement,
		readerRole: writerEntitlement,
		writerRole: viewerEntitlement,
	}
	datasetEntitlements = []string{ownerEntitlement, writerEntitlement, viewerEntitlement}
)

func (o *datasetBuilder) ResourceType(ctx context.Context) *v2.ResourceType {
	return datasetResourceType
}

func datasetResource(dataset string) (*v2.Resource, error) {
	profile := map[string]interface{}{
		"name": dataset,
	}

	datasetTraitOptions := []rs.AppTraitOption{
		rs.WithAppProfile(profile),
	}

	resource, err := rs.NewAppResource(dataset, datasetResourceType, dataset, datasetTraitOptions)
	if err != nil {
		return nil, err
	}

	return resource, nil
}

func (o *datasetBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	iter := o.bigQueryClient.Datasets(ctx)
	var resources []*v2.Resource
	for {
		dataset, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, "", nil, err
		}

		resource, err := datasetResource(dataset.DatasetID)
		if err != nil {
			return nil, "", nil, err
		}

		resources = append(resources, resource)
	}

	return resources, "", nil, nil
}

func (o *datasetBuilder) Entitlements(_ context.Context, resource *v2.Resource, _ *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	var rv []*v2.Entitlement

	var entitlementToVerbMap = map[string]string{
		ownerEntitlement:  "Owns",
		writerEntitlement: "Can write to",
		viewerEntitlement: "Can view",
	}
	for _, datasetEntitlement := range datasetEntitlements {
		assigmentOptions := []ent.EntitlementOption{
			ent.WithGrantableTo(userResourceType),
			ent.WithDescription(fmt.Sprintf("%s %s dataset", entitlementToVerbMap[datasetEntitlement], resource.DisplayName)),
			ent.WithDisplayName(fmt.Sprintf("%s dataset %s", resource.DisplayName, datasetEntitlement)),
		}
		rv = append(rv, ent.NewPermissionEntitlement(resource, datasetEntitlement, assigmentOptions...))

		assigmentOptions = []ent.EntitlementOption{
			ent.WithGrantableTo(serviceAccountResourceType),
			ent.WithDescription(fmt.Sprintf("%s %s dataset", entitlementToVerbMap[datasetEntitlement], resource.DisplayName)),
			ent.WithDisplayName(fmt.Sprintf("%s dataset %s", resource.DisplayName, datasetEntitlement)),
		}
		rv = append(rv, ent.NewPermissionEntitlement(resource, datasetEntitlement, assigmentOptions...))
	}

	assigmentOptions := []ent.EntitlementOption{
		ent.WithGrantableTo(roleResourceType),
		ent.WithDescription(fmt.Sprintf("has access to %s dataset", resource.DisplayName)),
		ent.WithDisplayName(fmt.Sprintf("%s dataset %s", resource.DisplayName, accessEntitlement)),
	}
	rv = append(rv, ent.NewAssignmentEntitlement(resource, accessEntitlement, assigmentOptions...))

	return rv, "", nil, nil
}

func (o *datasetBuilder) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	dataset, err := o.bigQueryClient.Dataset(resource.Id.Resource).Metadata(ctx)
	if err != nil {
		return nil, "", nil, err
	}

	policy, err := o.projectsClient.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{
		Resource: fmt.Sprintf("projects/%s", o.bigQueryClient.Project()),
	})
	if err != nil {
		return nil, "", nil, err // TODO: wrap error
	}

	var grants []*v2.Grant
	for _, access := range dataset.Access {
		g, err := o.GetUserOwnerGrants(resource, access) // TODO: think about this again
		if err != nil {
			return nil, "", nil, err
		}
		grants = append(grants, g...)

		stringLegacyRoleValue := string(access.Role)
		role, exists := legacyRolesMap[stringLegacyRoleValue]
		if !exists {
			return nil, "", nil, fmt.Errorf("role for legacy role %s not found", stringLegacyRoleValue)
		}

		e, exists := legacyRolesToEntitlementsMap[stringLegacyRoleValue]
		if !exists {
			return nil, "", nil, fmt.Errorf("entitlement for legacy role %s not found", stringLegacyRoleValue)
		}

		for _, binding := range policy.Bindings {
			if binding.Role != role {
				continue
			}

			roleResource, err := roleResource(binding.Role)
			if err != nil {
				return nil, "", nil, err
			}
			grants = append(grants, grant.NewGrant(resource, accessEntitlement, roleResource.Id))

			for _, member := range binding.Members {
				if isUser, user := isUser(member); isUser {
					userResource, err := userResource(user)
					if err != nil {
						return nil, "", nil, err
					}

					grants = append(grants, grant.NewGrant(resource, e, userResource.Id))
				} else if isServiceAccount, serviceAccount := isServiceAccount(member); isServiceAccount {
					serviceAccountResource, err := serviceAccountResource(serviceAccount)
					if err != nil {
						return nil, "", nil, err
					}

					grants = append(grants, grant.NewGrant(resource, e, serviceAccountResource.Id))
				}
			}
		}
	}

	return grants, "", nil, nil
}

func (o *datasetBuilder) GetUserOwnerGrants(resource *v2.Resource, access *bigquery.AccessEntry) ([]*v2.Grant, error) {
	if access.Role != bigquery.OwnerRole || access.EntityType != bigquery.UserEmailEntity {
		return []*v2.Grant{}, nil
	}

	userResource, err := userResource(access.Entity)
	if err != nil {
		return nil, err
	}

	return []*v2.Grant{
		grant.NewGrant(resource, ownerEntitlement, userResource.Id),
	}, nil
}

func newDatasetBuilder(bigQueryClient *bigquery.Client, projectsClient *resourcemanager.ProjectsClient) *datasetBuilder {
	return &datasetBuilder{
		resourceType:   datasetResourceType,
		bigQueryClient: bigQueryClient,
		projectsClient: projectsClient,
	}
}
