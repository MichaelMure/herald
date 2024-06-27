package herald

import (
	"github.com/ipfs/go-log/v2"
)

var (
	logger = log.Logger("herald")
)

// TODO: rework the options
// TODO: construct and assemble components based on options

type Herald struct {
	*options
	publisher *httpPublisher

	// backend
	// optional: publisher (http)
	// announcer (http, pubsub) - announce.Sender
	//

}

func New(o ...Option) (*Herald, error) {
	opts, err := newOptions(o...)
	if err != nil {
		return nil, err
	}
	h := &Herald{options: opts}
	// dspub, err := newDsPublisher(h)
	// if err != nil {
	// 	return nil, err
	// }
	// h.publisher, err = newHttpPublisher(h, dspub)
	// if err != nil {
	// 	return nil, err
	// }
	return h, err
}

//
// func (h *Herald) Start(ctx context.Context) error {
// 	return h.publisher.Start(ctx)
// }
//
// func (h *Herald) Shutdown(ctx context.Context) error {
// 	return h.publisher.Shutdown(ctx)
// }
