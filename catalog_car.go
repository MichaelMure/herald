package herald

import (
	"context"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multihash"
)

func CatalogFromCar(path string, id []byte) (*CarCatalog, error) {
	car, err := blockstore.OpenReadOnly(path)
	if err != nil {
		return nil, err
	}
	return &CarCatalog{car: car, id: id}, nil
}

var _ Catalog = &CarCatalog{}

type CarCatalog struct {
	car *blockstore.ReadOnly
	id  []byte
}

func (c *CarCatalog) ID() []byte {
	return c.id
}

func (c *CarCatalog) Count() int {
	idx, ok := c.car.Index().(index.IterableIndex)
	if !ok {
		return -1
	}
	var count int
	err := idx.ForEach(func(mh multihash.Multihash, u uint64) error {
		count++
		return nil
	})
	if err != nil {
		return -1
	}
	return count
}

func (c *CarCatalog) Iterator(ctx context.Context) (MhIterator, error) {
	cids, err := c.car.AllKeysChan(ctx)
	if err != nil {
		return nil, err
	}
	return &CarIterator{
		c: cids,
	}, nil
}

var _ MhIterator = &CarIterator{}

type CarIterator struct {
	c    <-chan cid.Cid
	next multihash.Multihash
}

func (c *CarIterator) Next() multihash.Multihash {
	return c.next
}

func (c *CarIterator) Done() bool {
	val, ok := <-c.c
	if ok {
		c.next = val.Hash()
		return true
	}
	return false
}
