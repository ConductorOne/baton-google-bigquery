package main

import (
	"github.com/conductorone/baton-google-bigquery/pkg/config"
	sdkConfig "github.com/conductorone/baton-sdk/pkg/config"
)

type GoogleBigQueryConfig struct {
	CredentialsJSONFilePath string `mapstructure:"credentials-json-file-path"`
}

func main() {
	sdkConfig.Generate("google-bigquery", config.Config)
}
