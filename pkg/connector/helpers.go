package connector

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/googleapis/gax-go/v2/apierror"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
)

const (
	iamPermissionDenied = "IAM_PERMISSION_DENIED"
	NF                  = -1
)

func wrapError(err error, message string) error {
	if message == "" {
		return fmt.Errorf("google-big-query-connector: %w", err)
	}
	return fmt.Errorf("google-big-query-connector: %s: %w", message, err)
}

func isPermissionDenied(ctx context.Context, err error) bool {
	var ae *apierror.APIError
	l := ctxzap.Extract(ctx)
	if errors.As(err, &ae) {
		if ae.GRPCStatus().Code() != codes.PermissionDenied {
			l.Error(
				"baton-google-bigquery: listing resource failed",
				zap.String("reason", ae.Reason()),
				zap.Any("grpc_status", ae.GRPCStatus().Err()),
				zap.Any("details", ae.Details().ErrorInfo),
			)

			return false
		}

		l.Error(
			"baton-google-bigquery: failed to list resources <PermissionDenied>",
			zap.String("reason", ae.Reason()),
			zap.Any("grpc_status", ae.GRPCStatus().Err()),
			zap.Any("details", ae.Details().ErrorInfo),
			zap.Any("error", err),
		)
	}

	return true
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

func serviceAccountResource(member string, parentResourceID *v2.ResourceId) (*v2.Resource, error) {
	profile := map[string]interface{}{
		"email": member,
	}
	serviceAccountTraits := []rs.AppTraitOption{
		rs.WithAppProfile(profile),
	}
	resource, err := rs.NewAppResource(member,
		serviceAccountResourceType,
		member,
		serviceAccountTraits,
		rs.WithParentResourceID(parentResourceID),
	)
	if err != nil {
		return nil, err
	}

	return resource, nil
}

func roleResource(role string, parentResourceID *v2.ResourceId) (*v2.Resource, error) {
	roleName := removeRolesPrefix(role)
	profile := map[string]interface{}{
		"name": roleName,
	}
	roleTraitOptions := []rs.RoleTraitOption{
		rs.WithRoleProfile(profile),
	}
	resource, err := rs.NewRoleResource(roleName,
		roleResourceType,
		role,
		roleTraitOptions,
		rs.WithParentResourceID(parentResourceID),
	)
	if err != nil {
		return nil, err
	}

	return resource, nil
}

func removeRolesPrefix(role string) string {
	return strings.TrimPrefix(role, "roles/")
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

func projectResource(projects *resourcemanagerpb.Project) (*v2.Resource, error) {
	var opts []rs.ResourceOption
	profile := map[string]interface{}{
		"id":          projects.ProjectId,
		"name":        projects.Name,
		"displayName": projects.DisplayName,
	}

	projectTraitOptions := []rs.AppTraitOption{
		rs.WithAppProfile(profile),
	}

	opts = append(opts, rs.WithAppTrait(projectTraitOptions...))
	resource, err := rs.NewResource(
		projects.DisplayName,
		projectResourceType,
		projects.ProjectId,
		opts...,
	)
	if err != nil {
		return nil, err
	}

	return resource, nil
}

func isUser(member string) (bool, string) {
	if strings.HasPrefix(member, "user:") {
		return true, strings.TrimPrefix(member, "user:")
	}

	return false, ""
}

func isServiceAccount(member string) (bool, string) {
	if strings.HasPrefix(member, "serviceAccount:") {
		return true, strings.TrimPrefix(member, "serviceAccount:")
	}

	return false, ""
}
