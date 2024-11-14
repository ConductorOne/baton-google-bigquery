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
	serviceAccount    = "serviceAccount"
	user              = "user"
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
		ownerRole:  "roles/bigquery.dataOwner",
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

func datasetResource(ctx context.Context, datasetName string, parentResourceID *v2.ResourceId) (*v2.Resource, error) {
	profile := map[string]interface{}{
		"name": datasetName,
	}

	groupTraitOptions := []rs.GroupTraitOption{rs.WithGroupProfile(profile)}
	resource, err := rs.NewGroupResource(
		datasetName,
		datasetResourceType,
		datasetName,
		groupTraitOptions,
		rs.WithParentResourceID(parentResourceID),
	)
	if err != nil {
		return nil, err
	}

	return resource, nil
}

func (o *datasetBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
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

	it := o.projectsClient.SearchProjects(ctx,
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

		iter := o.bigQueryClient.Datasets(ctx)
		// Setting ProjectID on the returned iterator
		iter.ProjectID = project.ProjectId
		for {
			dataset, err := iter.Next()
			if errors.Is(err, iterator.Done) || dataset == nil {
				break
			}

			if err != nil {
				return nil, "", nil, wrapError(err, "Unable to fetch dataset")
			}

			resource, err := datasetResource(ctx, dataset.DatasetID, &v2.ResourceId{
				ResourceType: projectResourceType.Id,
				Resource:     dataset.ProjectID,
			})
			if err != nil {
				return nil, "", nil, wrapError(err, "Unable to create dataset resource")
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

func isUserOrServiceAccount(policy *iampb.Policy, memberGranted string) string {
	for _, binding := range policy.Bindings {
		for _, member := range binding.Members {
			switch member {
			case fmt.Sprintf("%s:%s", serviceAccount, memberGranted):
				return serviceAccount
			case fmt.Sprintf("%s:%s", user, memberGranted):
				return user
			}
		}
	}

	return ""
}

func (o *datasetBuilder) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	var grants []*v2.Grant
	datasetID := resource.Id.Resource
	projectId := resource.ParentResourceId.Resource
	ds := o.bigQueryClient.DatasetInProject(projectId, datasetID)
	dataset, err := ds.Metadata(ctx)
	if err != nil {
		return nil, "", nil, wrapError(err, "Unable to fetch dataset metadata (projectId:"+projectId+" datasetID:"+datasetID+")")
	}

	policy, err := o.projectsClient.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{
		Resource: fmt.Sprintf("projects/%s", projectId),
	})

	if err != nil {
		if !isPermissionDenied(ctx, err) {
			return nil, "", nil, wrapError(err, "failed to get IAM policy")
		}
	}

	if policy == nil {
		return grants, "", nil, nil
	}

	for _, access := range dataset.Access {
		if (access.Role == bigquery.OwnerRole || access.EntityType == bigquery.UserEmailEntity) && access.EntityType != bigquery.SpecialGroupEntity {
			g, err := o.GetUserOwnerGrants(policy, resource, access)
			if err != nil {
				return nil, "", nil, err
			}
			grants = append(grants, g...)
		}

		stringLegacyRoleValue := string(access.Role)
		role, exists := legacyRolesMap[stringLegacyRoleValue]
		if !exists {
			return nil, "", nil, wrapError(fmt.Errorf("role for legacy role %s not found", stringLegacyRoleValue), "")
		}

		e, exists := legacyRolesToEntitlementsMap[stringLegacyRoleValue]
		if !exists {
			return nil, "", nil, wrapError(fmt.Errorf("entitlement for legacy role %s not found", stringLegacyRoleValue), "")
		}

		for _, binding := range policy.Bindings {
			if binding.Role != role {
				continue
			}

			g, err := o.GetRoleGrants(resource, role)
			if err != nil {
				return nil, "", nil, err
			}
			grants = append(grants, g...)

			for _, member := range binding.Members {
				if isUser, user := isUser(member); isUser {
					userResource, err := userResource(user, nil)
					if err != nil {
						return nil, "", nil, wrapError(err, "Unable to create user resource")
					}

					grants = append(grants, grant.NewGrant(resource, e, userResource.Id))
				} else if isServiceAccount, serviceAccount := isServiceAccount(member); isServiceAccount {
					serviceAccountResource, err := serviceAccountResource(serviceAccount, nil)
					if err != nil {
						return nil, "", nil, wrapError(err, "Unable to create service account resource")
					}

					grants = append(grants, grant.NewGrant(resource, e, serviceAccountResource.Id))
				}
			}
		}
	}

	return grants, "", nil, nil
}

func (o *datasetBuilder) GetUserOwnerGrants(policy *iampb.Policy, resource *v2.Resource, access *bigquery.AccessEntry) ([]*v2.Grant, error) {
	var (
		res *v2.Resource
		err error
	)
	switch isUserOrServiceAccount(policy, access.Entity) {
	case serviceAccount:
		res, err = serviceAccountResource(access.Entity, nil)
	case user:
		res, err = userResource(access.Entity, nil)
	}
	if err != nil {
		return nil, err
	}

	return []*v2.Grant{
		grant.NewGrant(resource, ownerEntitlement, res.Id),
	}, nil
}

func (o *datasetBuilder) GetRoleGrants(resource *v2.Resource, role string) ([]*v2.Grant, error) {
	roleResource, err := roleResource(role, nil)
	if err != nil {
		return nil, wrapError(err, "Unable to create role resource")
	}

	return []*v2.Grant{
		grant.NewGrant(resource, accessEntitlement, roleResource.Id),
	}, nil
}

func newDatasetBuilder(bigQueryClient *bigquery.Client, projectsClient *resourcemanager.ProjectsClient) *datasetBuilder {
	return &datasetBuilder{
		resourceType:   datasetResourceType,
		bigQueryClient: bigQueryClient,
		projectsClient: projectsClient,
	}
}
