package connector

import "fmt"

func wrapError(err error, message string) error {
	if message == "" {
		return fmt.Errorf("google-big-query-connector: %w", err)
	}
	return fmt.Errorf("google-big-query-connector: %s: %w", message, err)
}
