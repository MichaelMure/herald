package herald

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCarCatalog(t *testing.T) {
	id := []byte("fooo")
	cat, err := CatalogFromCar("testdata/1.car", id)
	require.NoError(t, err)
	require.Equal(t, 5, cat.Count())
	require.Equal(t, id, cat.ID())

	var count int

	iter, err := cat.Iterator(context.Background())
	require.NoError(t, err)

	for !iter.Done() {
		require.True(t, len(iter.Next().String()) > 0)
		count++
	}

	require.Equal(t, 5, count)
}
