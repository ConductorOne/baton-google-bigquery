package main

import (
	cfg "github.com/conductorone/baton-google-bigquery/pkg/config"
	"github.com/conductorone/baton-sdk/pkg/config"
)

func main() {
	config.Generate("googlebigquery", cfg.Configuration)
}
