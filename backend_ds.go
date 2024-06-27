package herald

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

var _ chainWriter = &dsBackend{}
var _ chainReader = &dsBackend{}

// dsBackend is an IPNI publishing backend that stores the chain in a datastore.Datastore.
type dsBackend struct {
	locker sync.RWMutex // atomicity over the chain head
	head   cid.Cid      // cache the head CID

	ds datastore.Datastore
	ls ipld.LinkSystem
}

func newDsPublisher(ds datastore.Datastore) *dsBackend {
	p := &dsBackend{ds: ds, head: cid.Undef}
	p.ls = cidlink.DefaultLinkSystem()
	p.ls.StorageReadOpener = p.storageReadOpener
	p.ls.StorageWriteOpener = p.storageWriteOpener
	return p
}

func (p *dsBackend) storageReadOpener(ctx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
	val, err := p.ds.Get(ctx.Ctx, dsKey(lnk))
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(val), nil
}

func (p *dsBackend) storageWriteOpener(linkCtx linking.LinkContext) (io.Writer, linking.BlockWriteCommitter, error) {
	buf := bytesBuffersPool.Get().(*bytes.Buffer)
	buf.Reset()
	return buf, func(lnk ipld.Link) error {
		defer bytesBuffersPool.Put(buf)
		return p.ds.Put(linkCtx.Ctx, dsKey(lnk), buf.Bytes())
	}, nil
}

func dsKey(l ipld.Link) datastore.Key {
	return datastore.NewKey(l.(cidlink.Link).Cid.String())
}

// UpdateHead perform an atomic update of the IPNI chain head
func (p *dsBackend) UpdateHead(ctx context.Context, fn func(prevHead cid.Cid) (cid.Cid, error)) error {
	p.locker.Lock()
	defer p.locker.Unlock()

	prevHead, err := p.getHead(ctx)
	if err != nil {
		return err
	}

	newHead, err := fn(prevHead)
	if err != nil {
		return err
	}

	return p.setHead(ctx, newHead)
}

var headKey = datastore.NewKey("head")

func (p *dsBackend) getHead(ctx context.Context) (cid.Cid, error) {
	if p.head != cid.Undef {
		return p.head, nil
	}

	switch value, err := p.ds.Get(ctx, headKey); {
	case errors.Is(err, datastore.ErrNotFound):
		return cid.Undef, nil
	case err != nil:
		return cid.Undef, err
	default:
		_, head, err := cid.CidFromBytes(value)
		if err != nil {
			logger.Errorw("failed to decode stored head as CID", "err", err)
			return cid.Undef, err
		}
		p.head = head
		return head, nil
	}
}

func (p *dsBackend) setHead(ctx context.Context, newHead cid.Cid) error {
	if !newHead.Defined() {
		// sanity check
		return fmt.Errorf("trying to set an undefined chain head")
	}

	if err := p.ds.Put(ctx, headKey, newHead.Bytes()); err != nil {
		logger.Errorw("failed to set new head", "newHead", newHead, "err", err)
		return err
	}
	p.head = newHead
	return nil
}

// Store record a new IPLD node into the backend
func (p *dsBackend) Store(lnkCtx linking.LinkContext, lp datamodel.LinkPrototype, n datamodel.Node) (datamodel.Link, error) {
	return p.ls.Store(lnkCtx, lp, n)
}

// GetHead return the cid of the IPNI chain head
// Returns cid.Undef if the chain hasn't started yet.
func (p *dsBackend) GetHead(ctx context.Context) (cid.Cid, error) {
	p.locker.RLock()
	defer p.locker.RUnlock()
	return p.getHead(ctx)
}

// GetContent returns the raw content of an IPLD block of the IPNI chain.
// Returns ErrContentNotFound if not found.
func (p *dsBackend) GetContent(ctx context.Context, cid cid.Cid) ([]byte, error) {
	key := dsKey(cidlink.Link{Cid: cid})
	switch value, err := p.ds.Get(ctx, key); {
	case errors.Is(err, datastore.ErrNotFound):
		return nil, ErrContentNotFound
	case err != nil:
		return nil, err
	default:
		return value, nil
	}
}

var bytesBuffersPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}
