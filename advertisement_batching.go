package herald

import (
	"context"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipni/go-libipni/announce"
	"github.com/multiformats/go-multihash"
)

// According to the IPNI specification, the maximum is:
//
//	> In terms of concrete constraints, each EntryChunk should stay below 4MB, and a linked list of entry chunks should be
//	> no more than 400 chunks long. Above these constraints, the list of entries should be split into multiple advertisements.
//	> Practically, this means that each individual advertisement can hold up to approximately 40 million multihashes.
//
// We, however, will remain lower to avoid memory spikes. There is almost zero upside for a higher value.
const DefaultMaxMHsPerAdvertisement = 200_000

const DefaultMaxDelay = 30 * time.Second

type BatchConfig struct {
	// CountThreshold is the threshold to separate two publishing strategies:
	// - above the threshold: publish as a single advertisement, with a ContextID for easy retraction
	// - below the threshold: batch together publishes and retract, with no ContextID
	CountThreshold int

	// MaxMHsPerAdvertisement is the maximum number of multihashes per advertisement, meaning per batch
	MaxMHsPerAdvertisement int

	// MaxDelay is the maximum delay after which a batch triggers
	MaxDelay time.Duration

	// allow overrides for testing
	publishWithContextID func(ctx context.Context, cfg ChainConfig, backend ChainWriter, catalog Catalog) (cid.Cid, error)
	retractWithContextID func(ctx context.Context, cfg ChainConfig, backend ChainWriter, catalog Catalog) (cid.Cid, error)
	publishRawMHs        func(ctx context.Context, cfg ChainConfig, backend ChainWriter, catalog Catalog) (cid.Cid, error)
	retractRawMHs        func(ctx context.Context, cfg ChainConfig, backend ChainWriter, catalog Catalog) (cid.Cid, error)
}

// CatalogBatcher is a batcher to publish/retract Catalog. Strategy is as follows:
// - above the threshold: publish as a single advertisement, with a ContextID for easy retraction
// - below the threshold: batch together publishes and retract, with no ContextID
type CatalogBatcher struct {
	batchConfig BatchConfig
	chainConfig ChainConfig
	backend     ChainWriter
	announcer   announce.Sender

	publish chan Catalog
	retract chan Catalog
}

func StartCatalogBatcher(batchConfig BatchConfig, chainCfg ChainConfig, backend ChainWriter, announcer announce.Sender) *CatalogBatcher {
	b := &CatalogBatcher{
		batchConfig: batchConfig,
		chainConfig: chainCfg,
		backend:     backend,
		announcer:   announcer,
		publish:     make(chan Catalog),
		retract:     make(chan Catalog),
	}

	publish := batchConfig.publishRawMHs
	if publish == nil {
		publish = PublishRawMHs
	}

	retract := batchConfig.retractRawMHs
	if retract == nil {
		retract = RetractRawMHs
	}

	go b.runBatcher(b.publish, publish)
	go b.runBatcher(b.retract, retract)

	return b
}

func (b *CatalogBatcher) PublishCatalog(ctx context.Context, catalog Catalog) error {
	if catalog.Count() > b.batchConfig.CountThreshold {
		publish := b.batchConfig.publishWithContextID
		if publish == nil {
			publish = PublishWithContextID
		}

		// for large catalogs, we don't do batching
		newHead, err := publish(ctx, b.chainConfig, b.backend, catalog)
		if err != nil {
			return err
		}
		return announce.Send(ctx, newHead, b.chainConfig.PublisherHttpAddrs, b.announcer)
	}

	select {
	case b.publish <- catalog:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *CatalogBatcher) RetractCatalog(ctx context.Context, catalog Catalog) error {
	if catalog.Count() > b.batchConfig.CountThreshold {
		retract := b.batchConfig.retractWithContextID
		if retract == nil {
			retract = RetractWithContextID
		}

		// for large catalogs, we don't do batching
		newHead, err := retract(ctx, b.chainConfig, b.backend, catalog)
		if err != nil {
			return err
		}
		return announce.Send(ctx, newHead, b.chainConfig.PublisherHttpAddrs, b.announcer)
	}

	select {
	case b.retract <- catalog:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *CatalogBatcher) runBatcher(ch chan Catalog, fn func(ctx context.Context, cfg ChainConfig, backend ChainWriter, catalog Catalog) (cid.Cid, error)) {
	var counter uint64
	var timer <-chan time.Time

	// pre-alloc to CountThreshold as a first reasonable approximation
	batch := make([]multihash.Multihash, 0, b.batchConfig.CountThreshold)

	send := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		defer func() {
			// reset the input
			batch = make([]multihash.Multihash, 0, b.batchConfig.CountThreshold)
		}()

		// kill the timer and drain the channel
		timer = nil

		// TODO: implement retry, otherwise we'd drop entirely the advertisements!
		newHead, err := fn(ctx, b.chainConfig, b.backend, CatalogFromMultihashes(batch...))
		if err != nil {
			logger.Errorw("failed to publish or retract batch", "err", err)
			return
		}

		err = announce.Send(ctx, newHead, b.chainConfig.PublisherHttpAddrs, b.announcer)
		if err != nil {
			logger.Errorw("failed to publish new head", "err", err, "head", newHead.String())
			return
		}
	}

	for {
		select {
		case <-timer:
			send()

		case catalog := <-ch:
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			iter, err := catalog.Iterator(ctx)
			cancel()
			if err != nil {
				logger.Errorw("failed to get catalog iterator", "err", err)
				continue
			}

			// Note: we always consume the whole catalog, even if that means overshooting the batch limit
			for !iter.Done() {
				batch = append(batch, iter.Next())
				counter++
			}

			if len(batch) >= b.batchConfig.MaxMHsPerAdvertisement {
				send()
				continue
			}

			// start the timer if needed
			if timer == nil {
				timer = time.After(b.batchConfig.MaxDelay)
			}
		}
	}
}
