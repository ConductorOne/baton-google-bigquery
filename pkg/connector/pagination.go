package connector

import (
	"strconv"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/pagination"
)

func parseOffsetFromPageToken(i string, resourceIDs ...*v2.ResourceId) (*pagination.Bag, int, error) {
	b := &pagination.Bag{}
	err := b.Unmarshal(i)
	if err != nil {
		return nil, 0, err
	}

	if b.Current() == nil {
		for _, resourceID := range resourceIDs {
			b.Push(pagination.PageState{
				ResourceTypeID: resourceID.ResourceType,
				ResourceID:     resourceID.Resource,
			})
		}
	}

	page, err := getOffsetFromPageToken(b.PageToken())
	if err != nil {
		return nil, 0, err
	}

	return b, page, nil
}

func getOffsetFromPageToken(token string) (int, error) {
	if token == "" {
		return 0, nil
	}

	page, err := strconv.ParseInt(token, 10, 64)
	if err != nil {
		return 0, err
	}

	return int(page), nil
}
