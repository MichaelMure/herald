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
	"github.com/multiformats/go-multihash"
)

type chainConfig struct {
	// adEntriesChunkSize is the maximum number of multihashes in a block of advertisement
	adEntriesChunkSize int

	// providerId is the libp2p identity of the IPNI publisher
	providerId peer.ID
	// providerKey is the keypair corresponding to providerId
	providerKey crypto.PrivKey

	// providerAddrs is the list of multiaddrs from which the content will be retrievable
	providerAddrs []string

	metadata []byte
}

// TODO: below strategy is purely:
// - publish: include all MHs and a ContextID
// - retract: no MHs, with the ContextID
// What we want:
// - above a threshold: publish and retract with ContextID
// - below that threshold: batch together small publish over a time window, publish without ContextID, retract with all MHs
// Note: batching could happen at the Catalog level, it doesn't have to be intertwined in the code below

// publishWithContextID generate the IPNI advertisement and chunks for the publishing of the given catalog.
// A ContextID is used as an identifier for easy retraction.
func publishWithContextID(ctx context.Context, cfg chainConfig, backend chainWriter, catalog Catalog) (cid.Cid, error) {
	if len(catalog.ID()) == 0 {
		return cid.Undef, fmt.Errorf("no valid ContextID to publish")
	}

	// generate the chain of chunks holding the multihashes
	entries, err := generateEntries(ctx, cfg, backend, catalog)
	if err != nil {
		return cid.Undef, err
	}
	// generate the root advertisement with all the metadata
	return generateAdvertisement(ctx, cfg, backend, catalog.ID(), entries, false)
}

// retractWithContextID generate the IPNI advertisement to retract the given catalog, using a ContextID.
func retractWithContextID(ctx context.Context, cfg chainConfig, backend chainWriter, catalog Catalog) (cid.Cid, error) {
	return generateAdvertisement(ctx, cfg, backend, catalog.ID(), schema.NoEntries, true)
}

// publishRawMHs generate the IPNI advertisement and chunks for the publishing of the given catalog, without ContextID.
func publishRawMHs(ctx context.Context, cfg chainConfig, backend chainWriter, catalog Catalog) (cid.Cid, error) {
	// generate the chain of chunks holding the multihashes
	entries, err := generateEntries(ctx, cfg, backend, catalog)
	if err != nil {
		return cid.Undef, err
	}
	// generate the root advertisement with all the metadata
	return generateAdvertisement(ctx, cfg, backend, nil, entries, false)
}

func retractRawMHs(ctx context.Context, cfg chainConfig, backend chainWriter, catalog Catalog) (cid.Cid, error) {
	// generate the chain of chunks holding the multihashes
	entries, err := generateEntries(ctx, cfg, backend, catalog)
	if err != nil {
		return cid.Undef, err
	}
	// generate the root retract advertisement with all the metadata
	return generateAdvertisement(ctx, cfg, backend, nil, entries, true)
}

// generateEntries produce all the linked chunks necessary to store the multihashes entry of the given catalog
func generateEntries(ctx context.Context, cfg chainConfig, backend chainWriter, catalog Catalog) (ipld.Link, error) {
	mhs := make([]multihash.Multihash, 0, cfg.adEntriesChunkSize)
	var next ipld.Link
	var mhCount, chunkCount int
	for iter := catalog.Iterator(); !iter.Done(); {
		mh, err := iter.Next()
		if err != nil {
			return nil, err
		}
		mhs = append(mhs, mh)
		mhCount++
		if len(mhs) >= cfg.adEntriesChunkSize {
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
func generateEntriesChunk(ctx context.Context, backend chainWriter, next ipld.Link, mhs []multihash.Multihash) (ipld.Link, error) {
	chunk, err := schema.EntryChunk{
		Entries: mhs,
		Next:    next,
	}.ToNode()
	if err != nil {
		return nil, err
	}
	return backend.Store(ipld.LinkContext{Ctx: ctx}, schema.Linkproto, chunk)
}

func generateAdvertisement(ctx context.Context, cfg chainConfig, backend chainWriter, id CatalogID, entries ipld.Link, isRm bool) (cid.Cid, error) {
	var newHead cid.Cid

	err := backend.UpdateHead(ctx, func(head cid.Cid) (cid.Cid, error) {
		var previousID ipld.Link
		if !cid.Undef.Equals(head) {
			previousID = cidlink.Link{Cid: head}
		}

		ad := schema.Advertisement{
			PreviousID: previousID,
			Provider:   cfg.providerId.String(),
			Addresses:  cfg.providerAddrs,
			Entries:    entries,
			ContextID:  id,
			Metadata:   cfg.metadata,
			IsRm:       isRm,
		}
		if err := ad.Sign(cfg.providerKey); err != nil {
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
