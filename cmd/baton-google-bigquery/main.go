package main

import (
	"context"
	"fmt"
	"os"

	"github.com/conductorone/baton-google-bigquery/pkg/connector"
	configSchema "github.com/conductorone/baton-sdk/pkg/config"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/field"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

const (
	version                 = "dev"
	connectorName           = "baton-google-bigquery"
	credentialsJSONFilePath = "credentials-json-file-path"
	projectsWhitelist       = "projects-whitelist"
)

var (
	credentialsJSONFilePathField = field.StringField(credentialsJSONFilePath, field.WithRequired(true), field.WithDescription("JSON credentials file name for the Google identity platform account."))
	projectsWhitelistField       = field.StringField(projectsWhitelist, field.WithRequired(false), field.WithDescription("List of project ids to sync."))
	configurationFields          = []field.SchemaField{credentialsJSONFilePathField, projectsWhitelistField}
)

func main() {
	ctx := context.Background()
	_, cmd, err := configSchema.DefineConfiguration(ctx,
		connectorName,
		getConnector,
		field.NewConfiguration(configurationFields),
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

func getConnector(ctx context.Context, cfg *viper.Viper) (types.ConnectorServer, error) {
	l := ctxzap.Extract(ctx)
	cb, err := connector.New(ctx,
		cfg.GetStringSlice(projectsWhitelist),
		cfg.GetString(credentialsJSONFilePath),
	)
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
