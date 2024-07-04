package herald

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	"github.com/ipni/go-libipni/announce"
	"github.com/ipni/go-libipni/announce/message"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

var _ ChainWriter = nilBackend{}

type nilBackend struct{}

func (n nilBackend) UpdateHead(ctx context.Context, fn func(prevHead cid.Cid) (cid.Cid, error)) error {
	return nil
}

func (n nilBackend) Store(lnkCtx linking.LinkContext, lp datamodel.LinkPrototype, node datamodel.Node) (datamodel.Link, error) {
	return nil, nil
}

var _ announce.Sender = nilAnnouncer{}

type nilAnnouncer struct{}

func (n nilAnnouncer) Close() error {
	return nil
}

func (n nilAnnouncer) Send(ctx context.Context, message message.Message) error {
	return nil
}

func TestBatching(t *testing.T) {
	const threshold = 10

	var publishWithContextID, retractWithContextID, publishRawMHs, retractRawMHs int64

	ctx := context.Background()

	cfg := BatchConfig{
		CountThreshold:         threshold,
		MaxMHsPerAdvertisement: 10,
		MaxDelay:               time.Second,
		publishWithContextID: func(ctx context.Context, cfg ChainConfig, backend ChainWriter, catalog Catalog) (cid.Cid, error) {
			atomic.AddInt64(&publishWithContextID, int64(catalog.Count()))
			return cid.Undef, nil
		},
		retractWithContextID: func(ctx context.Context, cfg ChainConfig, backend ChainWriter, catalog Catalog) (cid.Cid, error) {
			atomic.AddInt64(&retractWithContextID, int64(catalog.Count()))
			return cid.Undef, nil
		},
		publishRawMHs: func(ctx context.Context, cfg ChainConfig, backend ChainWriter, catalog Catalog) (cid.Cid, error) {
			atomic.AddInt64(&publishRawMHs, int64(catalog.Count()))
			return cid.Undef, nil
		},
		retractRawMHs: func(ctx context.Context, cfg ChainConfig, backend ChainWriter, catalog Catalog) (cid.Cid, error) {
			atomic.AddInt64(&retractRawMHs, int64(catalog.Count()))
			return cid.Undef, nil
		},
	}

	var counter int
	makeCatalog := func(size int) Catalog {
		mhs := make([]multihash.Multihash, 0, size)
		for i := 0; i < size; i++ {
			h, _ := multihash.Sum([]byte(strconv.Itoa(counter)), multihash.SHA2_256, -1)
			counter++
			mhs = append(mhs, h)
		}
		return CatalogFromMultihashes(mhs...)
	}

	batcher := StartCatalogBatcher(cfg, ChainConfig{}, nilBackend{}, nilAnnouncer{})

	// Publish: batch small catalogs
	for i := 0; i < 1000; i++ {
		err := batcher.PublishCatalog(ctx, makeCatalog(5))
		require.NoError(t, err)
	}
	eventuallyEqual(t, &publishRawMHs, 5000)

	// Publish: pass through large catalogs
	err := batcher.PublishCatalog(ctx, makeCatalog(1000))
	require.NoError(t, err)
	err = batcher.PublishCatalog(ctx, makeCatalog(1000))
	require.NoError(t, err)
	eventuallyEqual(t, &publishWithContextID, 2000)

	// Retract: batch small catalogs
	for i := 0; i < 1000; i++ {
		err := batcher.RetractCatalog(ctx, makeCatalog(5))
		require.NoError(t, err)
	}
	eventuallyEqual(t, &retractRawMHs, 5000)

	// Retract: pass through large catalogs
	err = batcher.RetractCatalog(ctx, makeCatalog(1000))
	require.NoError(t, err)
	err = batcher.RetractCatalog(ctx, makeCatalog(1000))
	require.NoError(t, err)
	eventuallyEqual(t, &retractWithContextID, 2000)

	require.Equal(t, int64(5000), publishRawMHs)
	require.Equal(t, int64(5000), retractRawMHs)
	require.Equal(t, int64(2000), publishWithContextID)
	require.Equal(t, int64(2000), retractWithContextID)
}

func eventuallyEqual(t *testing.T, i *int64, expected int64) {
	require.Eventually(t, func() bool {
		return atomic.LoadInt64(i) == expected
	}, 5*time.Second, 100*time.Millisecond)
}
