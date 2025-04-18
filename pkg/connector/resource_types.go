package connector

import (
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// The user resource type is for all user objects from the database.
var (
	userResourceType = &v2.ResourceType{
		Id:          "user",
		DisplayName: "User",
		Description: "User of Google Cloud Platform",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER},
	}
	roleResourceType = &v2.ResourceType{
		Id:          "role",
		DisplayName: "Role",
		Description: "Roles of Google BigQuery",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_ROLE},
	}
	datasetResourceType = &v2.ResourceType{
		Id:          "dataset",
		DisplayName: "Dataset",
		Description: "Dataset of Google BigQuery",
	}
	projectResourceType = &v2.ResourceType{
		Id:          "project",
		DisplayName: "Project",
		Description: "Project of Google BigQuery",
	}
)
