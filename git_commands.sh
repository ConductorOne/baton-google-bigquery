#!/bin/bash
cd /Users/laurenleach/go/src/github.com/ConductorOne/baton-google-bigquery
git add pkg/config
git add .github/workflows/capabilities_and_config.yaml
git add Makefile
git add cmd/baton-google-bigquery/main.go
git add .github/workflows/ci.yaml
git add .github/workflows/release.yaml
git add go.mod
git add pkg/connector/connector.go
git rm .github/workflows/capabilities.yaml
git rm .github/workflows/main.yaml
git status
