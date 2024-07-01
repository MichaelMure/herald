package herald

import "github.com/ipfs/boxo/blockstore"

func CatalogFromBlockstore(bs blockstore.Blockstore) *BsCatalog {
	return &BsCatalog{bs: bs}
}

var _ Catalog = BsCatalog{}

type BsCatalog struct {
	bs blockstore.Blockstore
}

func (b BsCatalog) ID() []byte {
	// TODO implement me
	panic("implement me")
}

func (b BsCatalog) Count() int {

}

func (b BsCatalog) Iterator() MhIterator {
	b.bs.AllKeysChan()
}
