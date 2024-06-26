package herald

import (
	"context"
	"errors"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
)

// chainBackend is a storage for an IPNI chain
type chainBackend interface {
	// UpdateHead perform an atomic update of the IPNI chain head
	UpdateHead(ctx context.Context, fn func(head cid.Cid) (cid.Cid, error)) error

	// Store record a new IPLD node into the backend
	Store(lnkCtx linking.LinkContext, lp datamodel.LinkPrototype, n datamodel.Node) (datamodel.Link, error)

	// TODO:
	//  - Update address
	//  - AddProvider
	//  - RemoveProvider
	//  - UpdateProvider
	//  - Transport et. al.
}

var ErrContentNotFound = errors.New("content is not found")

// chainBackendRaw is an optional interface allowing to attach a Publisher to a chainBackend
type chainBackendRaw interface {
	// GetHead return the cid of the IPNI chain head
	// Returns cid.Undef if the chain hasn't started yet.
	GetHead(ctx context.Context) (cid.Cid, error)

	// GetContent returns the raw content of an IPLD block of the IPNI chain.
	// Returns ErrContentNotFound if not found.
	GetContent(ctx context.Context, cid cid.Cid) ([]byte, error)
}
