package config

import (
	"github.com/conductorone/baton-sdk/pkg/field"
)

const (
	CredentialsJSONFilePath = "credentials-json-file-path"
)

var (
	credentialsJSONFilePathField = field.StringField(
		CredentialsJSONFilePath,
		field.WithDisplayName("Credentials JSON File Path"),
		field.WithRequired(true),
		field.WithDescription("JSON credentials file name for the Google identity platform account."),
	)

	Config = field.NewConfiguration(
		[]field.SchemaField{
			credentialsJSONFilePathField,
		},
		field.WithConnectorDisplayName("Google BigQuery"),
		field.WithHelpUrl("/docs/baton/google-bigquery"),
		field.WithIconUrl("/static/app-icons/google-bigquery.svg"),
	)
)
