`baton-google-bigquery` is a connector for Google BigQuery built using the [Baton SDK](https://github.com/conductorone/baton-sdk). It communicates with the Google API to sync data about users.

Check out [Baton](https://github.com/conductorone/baton) to learn more the project in general.

# Getting Started

## Prerequisites

Service account key for your project. If you don't already have one follow the steps [here](https://cloud.google.com/identity-platform/docs/install-admin-sdk#create-service-account-console) to create a service account and a service account key. If you already have a service account, make sure you have all the permissions set and APIs enabled. Then download the service key which will be used in the connector.

Make sure that used service account has either **Viewer** or **BigQuery Data Viewer** role.

## brew

```
brew install conductorone/baton/baton conductorone/baton/baton-google-bigquery
baton-google-bigquery
baton resources
```

## docker

```
docker run --rm -v $(pwd):/out -e BATON_CREDENTIALS_JSON_FILE_PATH=./pathOfServiceKey.json ghcr.io/conductorone/baton-google-bigquery:latest -f "/out/sync.c1z"
docker run --rm -v $(pwd):/out ghcr.io/conductorone/baton:latest -f "/out/sync.c1z" resources
```

## source

```
go install github.com/conductorone/baton/cmd/baton@main
go install github.com/conductorone/baton-google-bigquery/cmd/baton-google-bigquery@main

BATON_CREDENTIALS_JSON_FILE_PATH=./pathOfServiceKey.json
baton resources
```

# Data Model

`baton-google-bigquery` will pull down information about the following Google BigQuery resources:

- Users
- Service Accounts
- Datasets
- Roles

Note: For listing datasets, The required role is "BigQuery Data Editor".
# Contributing, Support and Issues

We started Baton because we were tired of taking screenshots and manually building spreadsheets. We welcome contributions, and ideas, no matter how small -- our goal is to make identity and permissions sprawl less painful for everyone. If you have questions, problems, or ideas: Please open a Github Issue!

See [CONTRIBUTING.md](https://github.com/ConductorOne/baton/blob/main/CONTRIBUTING.md) for more details.

# `baton-google-bigquery` Command Line Usage

```
baton-google-bigquery

Usage:
  baton-google-bigquery [flags]
  baton-google-bigquery [command]

Available Commands:
  capabilities       Get connector capabilities
  completion         Generate the autocompletion script for the specified shell
  help               Help about any command

Flags:
      --client-id string                    The client ID used to authenticate with ConductorOne ($BATON_CLIENT_ID)
      --client-secret string                The client secret used to authenticate with ConductorOne ($BATON_CLIENT_SECRET)
      --credentials-json-file-path string   required: JSON credentials file name for the Google identity platform account. ($BATON_CREDENTIALS_JSON_FILE_PATH)
  -f, --file string                         The path to the c1z file to sync with ($BATON_FILE) (default "sync.c1z")
  -h, --help                                help for baton-google-bigquery
      --log-format string                   The output format for logs: json, console ($BATON_LOG_FORMAT) (default "json")
      --log-level string                    The log level: debug, info, warn, error ($BATON_LOG_LEVEL) (default "info")
      --projects-whitelist string           List of project ids to sync. ($BATON_PROJECTS_WHITELIST)
  -p, --provisioning                        This must be set in order for provisioning actions to be enabled ($BATON_PROVISIONING)
      --skip-full-sync                      This must be set to skip a full sync ($BATON_SKIP_FULL_SYNC)
      --ticketing                           This must be set to enable ticketing support ($BATON_TICKETING)
  -v, --version                             version for baton-google-bigquery

Use "baton-google-bigquery [command] --help" for more information about a command.
```