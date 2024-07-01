package herald

import (
	"context"

	"github.com/multiformats/go-multihash"
)

func CatalogFromMultihashes(mhs ...multihash.Multihash) MhCatalog {
	return mhs
}

var _ Catalog = MhCatalog{}

type MhCatalog []multihash.Multihash

func (m MhCatalog) ID() []byte {
	return nil
}

func (m MhCatalog) Count() int {
	return len(m)
}

func (m MhCatalog) Iterator(_ context.Context) (MhIterator, error) {
	return mhIterator{catalog: m}, nil
}

var _ MhIterator = mhIterator{}

type mhIterator struct {
	catalog MhCatalog
	index   int
}

func (m mhIterator) Next(_ context.Context) (multihash.Multihash, error) {
	if m.Done() {
		panic("iterator already done")
	}
	defer func() { m.index++ }()
	return m.catalog[m.index], nil
}

func (m mhIterator) Done() bool {
	return m.index >= len(m.catalog)
}
