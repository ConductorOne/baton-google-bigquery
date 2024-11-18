package connector

import (
	"context"
	"errors"
	"fmt"

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
