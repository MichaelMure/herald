package herald

import (
	"github.com/multiformats/go-multihash"
)

func CatalogFromMultihashes(mhs ...multihash.Multihash) Catalog {
	return mhCatalog(mhs)
}

var _ Catalog = mhCatalog{}

type mhCatalog []multihash.Multihash

func (m mhCatalog) ID() []byte {
	return nil
}

func (m mhCatalog) Count() int {
	return len(m)
}

func (m mhCatalog) Iterator() MhIterator {
	return mhIterator{catalog: m}
}

var _ MhIterator = mhIterator{}

type mhIterator struct {
	catalog mhCatalog
	index   int
}

func (m mhIterator) Next() multihash.Multihash {
	if m.Done() {
		panic("iterator already done")
	}
	defer func() { m.index++ }()
	return m.catalog[m.index]
}

func (m mhIterator) Done() bool {
	return m.index >= len(m.catalog)
}
