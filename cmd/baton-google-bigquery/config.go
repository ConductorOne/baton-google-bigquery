package main

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-sdk/pkg/cli"
	"github.com/spf13/cobra"
)

// config defines the external configuration required for the connector to run.
type config struct {
	cli.BaseConfig `mapstructure:",squash"` // Puts the base config options in the same place as the connector options

	CredentialsJSONFilePath string `mapstructure:"credentials-json-file-path"`
}

// validateConfig is run after the configuration is loaded, and should return an error if it isn't valid.
func validateConfig(ctx context.Context, cfg *config) error {
	if cfg.CredentialsJSONFilePath == "" {
		return fmt.Errorf("credentials json file path is missing")
	}
	return nil
}

func cmdFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().String("credentials-json-file-path", "", "JSON credentials file name for the Google identity platform account. ($BATON_CREDENTIALS_JSON_FILE_PATH)")
}
