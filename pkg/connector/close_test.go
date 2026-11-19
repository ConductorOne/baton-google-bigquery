package connector

import (
	"context"
	"errors"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeConnector mirrors GoogleBigQuery's shape: a ConnectorBuilder carrying a
// context-free Close.
type fakeConnector struct {
	closeErr error
	calls    int
}

func (f *fakeConnector) ResourceSyncers(context.Context) []connectorbuilder.ResourceSyncer {
	return nil
}

func (f *fakeConnector) Metadata(context.Context) (*v2.ConnectorMetadata, error) {
	return &v2.ConnectorMetadata{}, nil
}

func (f *fakeConnector) Validate(context.Context) (annotations.Annotations, error) {
	return nil, nil
}

func (f *fakeConnector) Close() error {
	f.calls++
	return f.closeErr
}

// connectorbuilder selects the close hook through an unexported interface, so a
// connector's Close is never referenced directly and cannot be checked by the
// compiler. This pins that a context-free Close is still discovered and invoked.
func TestConnectorbuilderInvokesContextFreeCloseHook(t *testing.T) {
	ctx := context.Background()
	sentinel := errors.New("close failed")
	fake := &fakeConnector{closeErr: sentinel}

	c, err := connectorbuilder.NewConnector(ctx, fake)
	require.NoError(t, err)

	closer, ok := c.(interface{ Close(context.Context) error })
	require.True(t, ok, "ConnectorServer must expose Close for the CLI teardown path")

	assert.ErrorIs(t, closer.Close(ctx), sentinel, "the connector's Close error must surface")
	assert.Equal(t, 1, fake.calls, "the connector's Close must actually be invoked")
}

func TestCloseWithUnsetClientsDoesNotPanic(t *testing.T) {
	assert.NoError(t, (&GoogleBigQuery{}).Close())
}
