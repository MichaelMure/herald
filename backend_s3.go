package herald

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipni/go-libipni/dagsync/ipnisync/head"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/multiformats/go-multibase"
)

var _ chainBackend = &s3Backend{}

// s3Backend is a chainBackend storing the IPNI chain in S3, in a form that can directly be exposed publicly
// through HTTP. As such, it doesn't need an additional publisher.
type s3Backend struct {
	locker sync.RWMutex // atomicity over the chain head
	head   cid.Cid      // cache the head CID

	client *s3.Client
	bucket *string
	ls     ipld.LinkSystem

	// topic is the IPNI topic name on which the advertisement is published
	topic string
	// providerKey is the keypair of the IPNI publisher
	providerKey crypto.PrivKey
}

func newS3Backend(awsConfig aws.Config, bucket string, topic string, providerKey crypto.PrivKey) *s3Backend {
	// TODO: make client

	s := &s3Backend{
		bucket:      aws.String(bucket),
		topic:       topic,
		providerKey: providerKey,
	}
	s.ls = cidlink.DefaultLinkSystem()
	s.ls.StorageWriteOpener = s.storageWriteOpener
	return s
}

func (s *s3Backend) storageWriteOpener(linkCtx linking.LinkContext) (io.Writer, linking.BlockWriteCommitter, error) {
	buf := bytesBuffersPool.Get().(*bytes.Buffer)
	buf.Reset()
	return buf, func(lnk ipld.Link) error {
		defer bytesBuffersPool.Put(buf)

		c := lnk.(cidlink.Link).Cid

		key, err := c.StringOfBase(multibase.Base32)
		if err != nil {
			return err
		}

		var contentType string
		switch c.Prefix().Codec {
		case cid.DagJSON:
			contentType = "application/json"
		case cid.DagCBOR:
			contentType = "application/cbor"
		default:
			return fmt.Errorf("unknown block codec, cid %s, coded %v", c.String(), c.Prefix().Codec)
		}

		_, err = s.client.PutObject(linkCtx.Ctx, &s3.PutObjectInput{
			Bucket:       s.bucket,
			Key:          aws.String(key),
			Body:         buf,
			ContentType:  aws.String(contentType),
			CacheControl: aws.String("public, max-age=29030400, immutable"),
		})
		if err != nil {
			return err
		}
	}, nil
}

func (s *s3Backend) UpdateHead(ctx context.Context, fn func(head cid.Cid) (cid.Cid, error)) error {
	s.locker.Lock()
	defer s.locker.Unlock()

	prevHead, err := s.getHead(ctx)
	if err != nil {
		return err
	}

	newHead, err := fn(prevHead)
	if err != nil {
		return err
	}

	return s.setHead(ctx, newHead)
}

func (s *s3Backend) Store(lnkCtx linking.LinkContext, lp datamodel.LinkPrototype, n datamodel.Node) (datamodel.Link, error) {
	return s.ls.Store(lnkCtx, lp, n)
}

func (s *s3Backend) getHead(ctx context.Context) (cid.Cid, error) {
	if s.head != cid.Undef {
		return s.head, nil
	}

	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: s.bucket,
		Key:    aws.String("head"),
	})
	var noSuchKey *types.NoSuchKey
	if errors.As(err, &noSuchKey) {
		return cid.Undef, nil
	}
	if err != nil {
		return cid.Undef, err
	}

	decoded, err := head.Decode(out.Body)
	if err != nil {
		logger.Errorw("failed to decode stored head as SignedHead", "err", err)
		return cid.Undef, err
	}
	linkCid, ok := decoded.Head.(cidlink.Link)
	if !ok {
		logger.Errorw("unknown SignedHead type", "err", err)
		return cid.Undef, err
	}

	s.head = linkCid.Cid
	return s.head, nil
}

func (s *s3Backend) setHead(ctx context.Context, newHead cid.Cid) error {
	if !newHead.Defined() {
		// sanity check
		return fmt.Errorf("trying to set an undefined chain head")
	}

	signedHead, err := head.NewSignedHead(newHead, s.topic, s.providerKey)
	if err != nil {
		return fmt.Errorf("failed to generate signed head message")
	}
	encoded, err := signedHead.Encode()
	if err != nil {
		return fmt.Errorf("failed to encode signed head message")
	}

	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:       s.bucket,
		Key:          aws.String("head"),
		Body:         bytes.NewReader(encoded),
		ContentType:  aws.String("application/json"),
		CacheControl: aws.String("no-cache, no-store, must-revalidate"),
	})
	if err != nil {
		return err
	}

	s.head = newHead
	return nil
}
