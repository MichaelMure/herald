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
const defaultMaxMHsPerAdvertisement = 200_000

const defaultMaxDelay = 30 * time.Second

type batchConfig struct {
	// countThreshold is the threshold to separate two publishing strategies:
	// - above the threshold: publish as a single advertisement, with a ContextID for easy retraction
	// - below the threshold: batch together publishes and retract, with no ContextID
	countThreshold int

	// maxMHsPerAdvertisement is the maximum number of multihashes per advertisement, meaning per batch
	maxMHsPerAdvertisement int

	// maxDelay is the maximum delay after which a batch triggers
	maxDelay time.Duration
}

// catalogBatcher is a batcher to publish/retract Catalog. Strategy is as follows:
// - above the threshold: publish as a single advertisement, with a ContextID for easy retraction
// - below the threshold: batch together publishes and retract, with no ContextID
type catalogBatcher struct {
	batchConfig batchConfig
	chainConfig chainConfig
	backend     chainWriter
	announcer   announce.Sender

	publish chan Catalog
	retract chan Catalog
}

func startCatalogBatcher(batchConfig batchConfig, chainCfg chainConfig, backend chainWriter, announcer announce.Sender) *catalogBatcher {
	b := &catalogBatcher{
		batchConfig: batchConfig,
		chainConfig: chainCfg,
		backend:     backend,
		announcer:   announcer,
		publish:     make(chan Catalog),
		retract:     make(chan Catalog),
	}

	go b.runBatcher(b.publish, publishRawMHs)
	go b.runBatcher(b.retract, retractRawMHs)

	return b
}

func (b *catalogBatcher) PublishCatalog(ctx context.Context, catalog Catalog) error {
	if catalog.Count() > b.batchConfig.countThreshold {
		// for large catalogs, we don't do batching
		newHead, err := publishWithContextID(ctx, b.chainConfig, b.backend, catalog)
		if err != nil {
			return err
		}
		return announce.Send(ctx, newHead, b.chainConfig.publisherHttpAddrs, b.announcer)
	}

	select {
	case b.publish <- catalog:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *catalogBatcher) RetractCatalog(ctx context.Context, catalog Catalog) error {
	if catalog.Count() > b.batchConfig.countThreshold {
		// for large catalogs, we don't do batching
		newHead, err := retractWithContextID(ctx, b.chainConfig, b.backend, catalog)
		if err != nil {
			return err
		}
		return announce.Send(ctx, newHead, b.chainConfig.publisherHttpAddrs, b.announcer)
	}

	select {
	case b.retract <- catalog:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *catalogBatcher) runBatcher(ch chan Catalog, fn func(ctx context.Context, cfg chainConfig, backend chainWriter, catalog Catalog) (cid.Cid, error)) {
	var counter uint64
	var timer <-chan time.Time

	// pre-alloc to countThreshold as a first reasonable approximation
	batch := make([]multihash.Multihash, 0, b.batchConfig.countThreshold)

	send := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		defer func() {
			// reset the input
			batch = make([]multihash.Multihash, 0, b.batchConfig.countThreshold)
		}()

		// kill the timer and drain the channel
		timer = nil

		// TODO: implement retry, otherwise we'd drop entirely the advertisements!
		newHead, err := fn(ctx, b.chainConfig, b.backend, CatalogFromMultihashes(batch...))
		if err != nil {
			logger.Errorw("failed to publish or retract batch", "err", err)
			return
		}

		err = announce.Send(ctx, newHead, b.chainConfig.publisherHttpAddrs, b.announcer)
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
			// Note: we always consume the whole catalog, even if that means overshooting the batch limit
			for iter := catalog.Iterator(); !iter.Done(); {
				batch = append(batch, iter.Next())
				counter++
			}

			if len(batch) >= b.batchConfig.maxMHsPerAdvertisement {
				send()
				continue
			}

			// start the timer if needed
			if timer == nil {
				timer = time.After(b.batchConfig.maxDelay)
			}
		}
	}
}
