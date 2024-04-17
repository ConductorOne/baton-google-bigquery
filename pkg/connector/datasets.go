package connector

import (
	"context"
	"errors"

	"cloud.google.com/go/bigquery"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"google.golang.org/api/iterator"
)

type datasetBuilder struct {
	resourceType   *v2.ResourceType
	bigQueryClient *bigquery.Client
}

func (o *datasetBuilder) ResourceType(ctx context.Context) *v2.ResourceType {
	return datasetResourceType
}

func datasetResource(dataset string) (*v2.Resource, error) {
	profile := map[string]interface{}{
		"name": dataset,
	}

	datasetTraitOptions := []rs.AppTraitOption{
		rs.WithAppProfile(profile),
	}

	resource, err := rs.NewAppResource(dataset, datasetResourceType, dataset, datasetTraitOptions)
	if err != nil {
		return nil, err
	}

	return resource, nil
}

func (o *datasetBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	iter := o.bigQueryClient.Datasets(ctx)
	var resources []*v2.Resource
	for {
		dataset, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, "", nil, err
		}

		resource, err := datasetResource(dataset.DatasetID)
		if err != nil {
			return nil, "", nil, err
		}

		resources = append(resources, resource)
	}

	return resources, "", nil, nil
}

func (o *datasetBuilder) Entitlements(_ context.Context, resource *v2.Resource, _ *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func (o *datasetBuilder) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func newDatasetBuilder(bigQueryClient *bigquery.Client) *datasetBuilder {
	return &datasetBuilder{
		resourceType:   datasetResourceType,
		bigQueryClient: bigQueryClient,
	}
}
