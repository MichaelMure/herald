package herald

import (
	"github.com/multiformats/go-multihash"
)

// CatalogID is an identifier for a Catalog
type CatalogID []byte

// Catalog represent a single unit of multihashes to publish or retract
type Catalog interface {
	// ID returns a unique identifier for this catalog, that can be used as an IPNI ContextID.
	// Returning nil is allowed, to signify that there is no ContextID.
	ID() []byte

	// Iterator returns an iterator for the multihashes.
	Iterator() MhIterator
}

// MhIterator is an iterator over the collection of multihashes
type MhIterator interface {
	Next() (multihash.Multihash, error)
	Done() bool
}
