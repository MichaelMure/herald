package herald

import (
	"context"

	"github.com/multiformats/go-multihash"
)

// CatalogID is an identifier for a Catalog
type CatalogID []byte

// Catalog represent a single unit of multihashes to publish or retract
type Catalog interface {
	// ID returns a unique identifier for this catalog, that can be used as an IPNI ContextID.
	// Returning nil is allowed, to signify that there is no reasonable ContextID.
	ID() []byte

	// Count return the total number of multihashes, if known.
	// If not, -1 should be returned.
	Count() int

	// Iterator returns an iterator for the multihashes.
	Iterator(ctx context.Context) (MhIterator, error)
}

// MhIterator is an iterator over the collection of multihashes
type MhIterator interface {
	// Next returns the next multihash.
	Next() multihash.Multihash

	// Done returns true if there is no more multihash.
	Done() bool
}
