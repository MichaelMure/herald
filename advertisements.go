package herald

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipni/go-libipni/ingest/schema"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
)

type ChainConfig struct {
	// AdEntriesChunkSize is the maximum number of multihashes in a block of advertisement
	AdEntriesChunkSize int

	// ProviderId is the libp2p identity of the IPNI publisher
	ProviderId peer.ID
	// ProviderKey is the keypair corresponding to ProviderId
	ProviderKey crypto.PrivKey

	// ProviderAddrs is the list of multiaddrs from which the content will be retrievable
	ProviderAddrs []string

	// PublisherHttpAddrs is the HTTP addresses from which the IPNI chain is available
	PublisherHttpAddrs []multiaddr.Multiaddr

	Metadata []byte
}

// PublishWithContextID generate the IPNI advertisement and chunks for the publishing of the given catalog.
// A ContextID is used as an identifier for easy retraction.
func PublishWithContextID(ctx context.Context, cfg ChainConfig, backend ChainWriter, catalog Catalog) (cid.Cid, error) {
	if len(catalog.ID()) == 0 {
		return cid.Undef, fmt.Errorf("no valid ContextID to publish")
	}

	// generate the chain of chunks holding the multihashes
	entries, err := generateEntries(ctx, cfg, backend, catalog)
	if err != nil {
		return cid.Undef, err
	}
	// generate the root advertisement with all the Metadata
	return generateAdvertisement(ctx, cfg, backend, catalog.ID(), entries, false)
}

// RetractWithContextID generate the IPNI advertisement to retract the given catalog, using a ContextID.
func RetractWithContextID(ctx context.Context, cfg ChainConfig, backend ChainWriter, catalog Catalog) (cid.Cid, error) {
	return generateAdvertisement(ctx, cfg, backend, catalog.ID(), schema.NoEntries, true)
}

// PublishRawMHs generate the IPNI advertisement and chunks for the publishing of the given catalog, without ContextID.
func PublishRawMHs(ctx context.Context, cfg ChainConfig, backend ChainWriter, catalog Catalog) (cid.Cid, error) {
	// generate the chain of chunks holding the multihashes
	entries, err := generateEntries(ctx, cfg, backend, catalog)
	if err != nil {
		return cid.Undef, err
	}
	// generate the root advertisement with all the Metadata
	return generateAdvertisement(ctx, cfg, backend, nil, entries, false)
}

// RetractRawMHs generate the IPNI advertisement and chunks for the retraction of the given catalog, without ContextID.
func RetractRawMHs(ctx context.Context, cfg ChainConfig, backend ChainWriter, catalog Catalog) (cid.Cid, error) {
	// generate the chain of chunks holding the multihashes
	entries, err := generateEntries(ctx, cfg, backend, catalog)
	if err != nil {
		return cid.Undef, err
	}
	// generate the root retract advertisement with all the Metadata
	return generateAdvertisement(ctx, cfg, backend, nil, entries, true)
}

// generateEntries produce all the linked chunks necessary to store the multihashes entry of the given catalog
func generateEntries(ctx context.Context, cfg ChainConfig, backend ChainWriter, catalog Catalog) (ipld.Link, error) {
	mhs := make([]multihash.Multihash, 0, cfg.AdEntriesChunkSize)

	var err error
	var next ipld.Link
	var mh multihash.Multihash
	var mhCount, chunkCount int

	iter, err := catalog.Iterator(ctx)
	if err != nil {
		return nil, err
	}
	for !iter.Done() {
		mh, err = iter.Next(ctx)
		if err != nil {
			return nil, err
		}
		mhs = append(mhs, mh)
		mhCount++
		if len(mhs) >= cfg.AdEntriesChunkSize {
			next, err = generateEntriesChunk(ctx, backend, next, mhs)
			if err != nil {
				return nil, err
			}
			chunkCount++
			mhs = mhs[:0]
		}
	}
	if len(mhs) != 0 {
		var err error
		next, err = generateEntriesChunk(ctx, backend, next, mhs)
		if err != nil {
			return nil, err
		}
		chunkCount++
	}
	logger.Infow("Generated linked chunks of multihashes", "link", next, "totalMhCount", mhCount, "chunkCount", chunkCount)
	return next, nil
}

// generateEntriesChunk produce a single multihashes entry chunk containing mhs.
// If next is not nil, the produced chunk will be chained with next.
func generateEntriesChunk(ctx context.Context, backend ChainWriter, next ipld.Link, mhs []multihash.Multihash) (ipld.Link, error) {
	chunk, err := schema.EntryChunk{
		Entries: mhs,
		Next:    next,
	}.ToNode()
	if err != nil {
		return nil, err
	}
	return backend.Store(ipld.LinkContext{Ctx: ctx}, schema.Linkproto, chunk)
}

// generateAdvertisement produce an advertisement for the given chunk entries.
func generateAdvertisement(ctx context.Context, cfg ChainConfig, backend ChainWriter, id CatalogID, entries ipld.Link, isRm bool) (cid.Cid, error) {
	var newHead cid.Cid

	err := backend.UpdateHead(ctx, func(head cid.Cid) (cid.Cid, error) {
		var previousID ipld.Link
		if !cid.Undef.Equals(head) {
			previousID = cidlink.Link{Cid: head}
		}

		ad := schema.Advertisement{
			PreviousID: previousID,
			Provider:   cfg.ProviderId.String(),
			Addresses:  cfg.ProviderAddrs,
			Entries:    entries,
			ContextID:  id,
			Metadata:   cfg.Metadata,
			IsRm:       isRm,
		}
		if err := ad.Sign(cfg.ProviderKey); err != nil {
			logger.Errorw("failed to sign advertisement", "err", err)
			return cid.Undef, err
		}
		adNode, err := ad.ToNode()
		if err != nil {
			logger.Errorw("failed to generate IPLD node from advertisement", "err", err)
			return cid.Undef, err
		}
		adLink, err := backend.Store(ipld.LinkContext{Ctx: ctx}, schema.Linkproto, adNode)
		if err != nil {
			logger.Errorw("failed to store advertisement", "err", err)
			return cid.Undef, err
		}

		newHead = adLink.(cidlink.Link).Cid
		return newHead, nil
	})
	return newHead, err
}
