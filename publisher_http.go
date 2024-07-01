package herald

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipni/go-libipni/dagsync/ipnisync/head"
	"github.com/libp2p/go-libp2p/core/crypto"
)

// HttpPublisher is an IPNI HTTP publisher that exposes the IPNI chain for retrieval.
// It uses a ChainWriter as storage and renders the records on demand.
type HttpPublisher struct {
	backend ChainReader
	server  http.Server

	// topic is the IPNI topic name on which the advertisement is published
	topic string
	// providerKey is the keypair of the IPNI publisher
	providerKey crypto.PrivKey
}

func NewHttpPublisher(backend ChainReader, listenAddr string, topic string, providerKey crypto.PrivKey) (*HttpPublisher, error) {
	pub := &HttpPublisher{
		backend: backend,
		server: http.Server{
			Addr:              listenAddr,
			ReadTimeout:       10 * time.Second,
			ReadHeaderTimeout: 10 * time.Second,
			WriteTimeout:      10 * time.Second,
		},
		topic:       topic,
		providerKey: providerKey,
	}
	pub.server.Handler = pub.serveMux()
	return pub, nil
}

func (p *HttpPublisher) Start() error {
	listener, err := net.Listen("tcp", p.server.Addr)
	if err != nil {
		return err
	}
	go func() {
		if err := p.server.Serve(listener); errors.Is(err, http.ErrServerClosed) {
			logger.Info("HTTP publisher stopped successfully.")
		} else {
			logger.Errorw("HTTP publisher stopped erroneously.", "err", err)
		}
	}()
	logger.Infow("HTTP publisher started successfully.", "address", listener.Addr())
	return nil
}

func (p *HttpPublisher) serveMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/head", p.handleGetHead)
	mux.HandleFunc("/*", p.handleGetContent)
	return mux
}

func (p *HttpPublisher) handleGetHead(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	h, err := p.backend.GetHead(r.Context())
	if err != nil {
		logger.Errorw("failed to get head CID", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}
	if cid.Undef.Equals(h) {
		http.Error(w, "", http.StatusNoContent)
		return
	}
	signedHead, err := head.NewSignedHead(h, p.topic, p.providerKey)
	if err != nil {
		logger.Errorw("failed to generate signed head message", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}
	resp, err := signedHead.Encode()
	if err != nil {
		logger.Errorw("failed to encode signed head message", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if written, err := w.Write(resp); err != nil {
		logger.Errorw("failed to write encoded head response", "written", written, "err", err)
	} else {
		logger.Debugw("successfully responded with head message", "head", h, "written", written)
	}
}

func (p *HttpPublisher) handleGetContent(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	pathParam := strings.TrimPrefix("/", r.URL.RawPath)
	id, err := cid.Decode(pathParam)
	if err != nil {
		logger.Debugw("invalid CID as path parameter while getting content", "pathParam", pathParam, "err", err)
		http.Error(w, "invalid CID", http.StatusBadRequest)
		return
	}
	content, err := p.backend.GetContent(r.Context(), id)
	if errors.Is(err, ErrContentNotFound) {
		http.Error(w, "", http.StatusNotFound)
		return
	}
	if err != nil {
		logger.Errorw("failed to get content from store", "id", id, "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	switch id.Prefix().Codec {
	case cid.DagJSON:
		w.Header().Set("Content-Type", "application/json")
	case cid.DagCBOR:
		w.Header().Set("Content-Type", "application/cbor")
	default:
		logger.Errorw("unknown block codec", "cid", id.String(), "codec", id.Prefix().Codec)
		http.Error(w, "invalid block", http.StatusInternalServerError)
		return
	}

	_, err = w.Write(content)
	if err != nil {
		logger.Errorw("failed to write content response", "err", err)
	}
}

func (p *HttpPublisher) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return p.server.Shutdown(ctx)
}
