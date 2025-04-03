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
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

type datasetBuilder struct {
	resourceType   *v2.ResourceType
	bigQueryClient *bigquery.Client
	projectsClient *resourcemanager.ProjectsClient
}

const (
	adminEntitlement                = "roles/admin"
	editorEntitlement               = "roles/editor"
	readerEntitlement               = "reader"
	ownerEntitlement                = "owner"
	writerEntitlement               = "writer"
	viewerEntitlement               = "roles/viewer"
	bqAdminEntitlement              = "roles/bigquery.admin"
	bqStudioAdminEntitlement        = "roles/bigquery.studioAdmin"
	bqUserEntitlement               = "roles/bigquery.user"
	bqResourceEditorEntitlement     = "roles/bigquery.resourceEditor"
	bqMetadataViewerEntitlement     = "roles/bigquery.metadataViewer"
	bqFilteredDataViewerEntitlement = "roles/bigquery.filteredDataViewer"
	serviceAccount                  = "serviceAccount"
	user                            = "user"
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
	ownerRole  = string(bigquery.OwnerRole)
	readerRole = string(bigquery.ReaderRole)
	writerRole = string(bigquery.WriterRole)

	legacyRolesToEntitlementsMap = map[string]string{
		ownerRole:  ownerEntitlement,
		readerRole: viewerEntitlement,
		writerRole: writerEntitlement,
	}

	specialGroupNameToPolicyBindingRoleMap = map[string]string{
		"projectOwners":  "roles/owner",
		"projectReaders": "roles/viewer",
		"projectWriters": "roles/editor",
	}

	iamRoleToEntitlementMap = map[string]string{
		"roles/bigquery.admin":              bqAdminEntitlement,
		"roles/bigquery.studioAdmin":        bqStudioAdminEntitlement,
		"roles/bigquery.user":               bqUserEntitlement,
		"roles/bigquery.resourceEditor":     bqResourceEditorEntitlement, // Can read and modify dataset metadata but not the dataset content it self.
		"roles/bigquery.metadataViewer":     bqMetadataViewerEntitlement,
		"roles/bigquery.filteredDataViewer": bqFilteredDataViewerEntitlement, // Restricted Read Access. Can only access table rows which match their policy.
		"roles/admin":                       adminEntitlement,
		"roles/editor":                      editorEntitlement,
		"roles/reader":                      readerEntitlement,
	}
	datasetEntitlements        = []string{ownerEntitlement, writerEntitlement, viewerEntitlement}
	datasetIamRoleEntitlements = []string{
		bqAdminEntitlement,
		bqStudioAdminEntitlement,
		bqUserEntitlement,
		bqResourceEditorEntitlement,
		bqMetadataViewerEntitlement,
		bqFilteredDataViewerEntitlement,
		adminEntitlement,
		editorEntitlement,
		readerEntitlement,
	}
)

func (o *datasetBuilder) ResourceType(ctx context.Context) *v2.ResourceType {
	return datasetResourceType
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

		if err != nil {
			if !isPermissionDenied(ctx, err) {
				return nil, "", nil, wrapError(err, "Unable to fetch projects")
			}
		}

		iter := o.bigQueryClient.Datasets(ctx)
		iter.ProjectID = project.ProjectId // Setting ProjectID on the returned iterator
		for {
			dataset, err := iter.Next()
			if errors.Is(err, iterator.Done) || dataset == nil {
				break
			}

			if err != nil {
				if !isPermissionDenied(ctx, err) {
					return nil, "", nil, wrapError(err, "Unable to fetch dataset")
				}
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

	var iamRoleEntitlementVerb = "Has role"

	for _, datasetEntitlement := range datasetEntitlements {
		assigmentOptions := []ent.EntitlementOption{
			ent.WithGrantableTo(userResourceType),
			ent.WithDescription(fmt.Sprintf("%s %s dataset", entitlementToVerbMap[datasetEntitlement], resource.DisplayName)),
			ent.WithDisplayName(fmt.Sprintf("%s dataset %s", resource.DisplayName, datasetEntitlement)),
		}
		rv = append(rv, ent.NewPermissionEntitlement(resource, datasetEntitlement, assigmentOptions...))
	}

	for _, iamRoleEntitlement := range datasetIamRoleEntitlements {
		assigmentOptions := []ent.EntitlementOption{
			ent.WithGrantableTo(userResourceType),
			ent.WithDescription(fmt.Sprintf("%s %s in %s dataset", iamRoleEntitlementVerb, iamRoleEntitlement, resource.DisplayName)),
			ent.WithDisplayName(fmt.Sprintf("%s dataset %s", resource.DisplayName, iamRoleEntitlement)),
		}
		rv = append(rv, ent.NewPermissionEntitlement(resource, iamRoleEntitlement, assigmentOptions...))
	}
	return rv, "", nil, nil
}

func (o *datasetBuilder) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	var grants []*v2.Grant
	l := ctxzap.Extract(ctx)
	datasetID := resource.Id.Resource
	projectId := resource.ParentResourceId.Resource
	ds := o.bigQueryClient.DatasetInProject(projectId, datasetID)
	dataset, err := ds.Metadata(ctx)
	if err != nil {
		if !isPermissionDenied(ctx, err) {
			// Check if it's a 404 error from the Google API, based on DatasetsGetCall's Do we can check if the error is a 404 error
			// https://github.com/googleapis/google-api-go-client/blob/ca845161fd6688acdf5818fcb06f91d314866e4c/bigquery/v2/bigquery-gen.go#L10848-L10851
			var apiErr *googleapi.Error
			if errors.As(err, &apiErr) && apiErr.Code == 404 {
				// For 404 errors, just return nil without error
				l.Debug("Dataset not found (projectId:" + projectId + " datasetID:" + datasetID + ")")
				return nil, "", nil, nil
			}
			return nil, "", nil, wrapError(err, "Unable to fetch dataset metadata (projectId:"+projectId+" datasetID:"+datasetID+")")
		}
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
		stringLegacyRoleValue := string(access.Role)

		switch access.EntityType {
		case bigquery.UserEmailEntity:
			// An email address of a user to grant access to. For example: fred@example.com. Maps to IAM policy member "user:EMAIL" or "serviceAccount:EMAIL".
			if access.Role == bigquery.OwnerRole {
				// Generate Owners grants.
				g, err := o.GetUserOwnerGrants(policy, resource, access)
				if err != nil {
					l.Warn("error while creating user owner grant",
						zap.String("error", err.Error()))
					continue
				}
				grants = append(grants, g...)
			} else {
				roleEntitlement, exists := legacyRolesToEntitlementsMap[stringLegacyRoleValue]
				if !exists {
					roleEntitlement, exists = iamRoleToEntitlementMap[stringLegacyRoleValue]
					if !exists {
						l.Warn("Role is not a legacy nor a predifined IAM role with permissions to read or write datasets",
							zap.String("role", stringLegacyRoleValue))
						continue
					}
				}

				g, err := o.GetEntityGrant(policy, resource, access, roleEntitlement)
				if err != nil {
					l.Warn("error while creating user/acccount service grant",
						zap.String("error", err.Error()))
					continue
				}
				grants = append(grants, g...)
			}
		case bigquery.SpecialGroupEntity:
			// A special group to grant access to. Possible values include:
			//  - projectOwners: Owners of the enclosing project.
			//  - projectReaders: Readers of the enclosing project.
			//  - projectWriters: Writers of the enclosing project.
			//  - allAuthenticatedUsers: All authenticated BigQuery users.
			// Maps to similarly-named IAM members.
			e, exists := legacyRolesToEntitlementsMap[stringLegacyRoleValue]
			if !exists {
				l.Warn("entitlement for legacy role not found",
					zap.String("legacy role", stringLegacyRoleValue))
				continue
			}
			for _, binding := range policy.Bindings {
				specialGroupName := access.Entity
				role, exists := specialGroupNameToPolicyBindingRoleMap[specialGroupName]
				if !exists {
					l.Warn("Special group not found",
						zap.String("special group", specialGroupName))
					continue
				}
				if binding.Role != role {
					continue
				}
				for _, member := range binding.Members {
					var usrR *v2.Resource

					if isUser, user := isUser(member); isUser {
						usrR, err = userResource(user, nil, nil)
						if err != nil {
							l.Warn("Unable to create user resource",
								zap.String("error", err.Error()))
							continue
						}
					} else if isServiceAccount, serviceAccount := isServiceAccount(member); isServiceAccount {
						usrR, err = userResource(serviceAccount, nil, nil)
						if err != nil {
							l.Warn("Unable to create (service account) user resource",
								zap.String("error", err.Error()))
							continue
						}
					} else {
						continue
					}
					grants = append(grants, grant.NewGrant(resource, e, usrR.Id))
				}
			}
		default:
			// It's either groupByEmail, domain, view, routine, other dataset or another type of iamMember (not user or special group).
			l.Info("Skipping Access entry for unhandled entity type")
		}
	}

	return grants, "", nil, nil
}

func (o *datasetBuilder) GetEntityGrant(policy *iampb.Policy, resource *v2.Resource, access *bigquery.AccessEntry, entitlement string) ([]*v2.Grant, error) {
	var (
		res *v2.Resource
		err error
	)
	if isUserOrServiceAccount(policy, access.Entity) {
		res, err = userResource(access.Entity, nil, nil)
	} else {
		return nil, wrapError(fmt.Errorf("unknown entity type %s", access.Entity), "")
	}

	if err != nil {
		return nil, err
	}

	return []*v2.Grant{
		grant.NewGrant(resource, entitlement, res.Id),
	}, nil
}

func (o *datasetBuilder) GetUserOwnerGrants(policy *iampb.Policy, resource *v2.Resource, access *bigquery.AccessEntry) ([]*v2.Grant, error) {
	return o.GetEntityGrant(policy, resource, access, ownerEntitlement)
}

func newDatasetBuilder(bigQueryClient *bigquery.Client, projectsClient *resourcemanager.ProjectsClient) *datasetBuilder {
	return &datasetBuilder{
		resourceType:   datasetResourceType,
		bigQueryClient: bigQueryClient,
		projectsClient: projectsClient,
	}
}
