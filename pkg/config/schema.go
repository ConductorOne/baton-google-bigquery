package config

import (
	"github.com/conductorone/baton-sdk/pkg/field"
)

const (
	CredentialsJSONFilePath = "credentials-json-file-path"
)

var (
	credentialsJSONFilePathField = field.StringField(CredentialsJSONFilePath, field.WithRequired(true), field.WithDescription("JSON credentials file name for the Google identity platform account."))

	Config = field.Configuration{
		Fields: []field.SchemaField{
			credentialsJSONFilePathField,
		},
	}
)
