package main

import (
	"context"

	cfg "github.com/conductorone/baton-google-bigquery/pkg/config"
	"github.com/conductorone/baton-google-bigquery/pkg/connector"
	"github.com/conductorone/baton-sdk/pkg/config"
)

var version = "dev"

func main() {
	ctx := context.Background()
	config.RunConnector(ctx, "baton-google-bigquery", version, cfg.Config, connector.NewConnector)
}
