package config

import (
	"github.com/conductorone/baton-sdk/pkg/field"
)

var (
	CredentialsJSONFilePath = field.StringField(
		"credentials-json-file-path",
		field.WithRequired(true),
		field.WithDescription("JSON credentials file name for the Google identity platform account."),
		field.WithDisplayName("Credentials JSON File Path"),
	)

	// FieldRelationships defines relationships between the fields.
	FieldRelationships = []field.SchemaFieldRelationship{}
)

//go:generate go run ./gen
var Configuration = field.NewConfiguration([]field.SchemaField{
	CredentialsJSONFilePath,
}, field.WithConstraints(FieldRelationships...))
