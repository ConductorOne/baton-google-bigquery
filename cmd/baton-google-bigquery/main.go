package main

import (
	"context"
	"fmt"
	"os"

	"github.com/conductorone/baton-google-bigquery/pkg/config"
	"github.com/conductorone/baton-google-bigquery/pkg/connector"
	configSchema "github.com/conductorone/baton-sdk/pkg/config"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

var version = "dev"

func main() {
	ctx := context.Background()
	_, cmd, err := configSchema.DefineConfiguration(
		ctx,
		"baton-google-bigquery",
		getConnector,
		config.Configuration,
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	cmd.Version = version
	err = cmd.Execute()
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func getConnector(ctx context.Context, cfg *config.Googlebigquery) (types.ConnectorServer, error) {
	l := ctxzap.Extract(ctx)
	cb, err := connector.New(ctx, cfg.CredentialsJsonFilePath)
	if err != nil {
		l.Error("error creating connector", zap.Error(err))
		return nil, err
	}

	c, err := connectorbuilder.NewConnector(ctx, cb)
	if err != nil {
		l.Error("error creating connector", zap.Error(err))
		return nil, err
	}

	return c, nil
}
