package herald

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/ipni/go-libipni/announce/httpsender"
)

func Example() {
	awsCfg := aws.NewConfig()

	backend := NewS3Backend(awsCfg, "bucket", "/indexer/ingest/mainnet", keys)

	announcer := httpsender.New()

	batcher := StartCatalogBatcher(BatchConfig{
		countThreshold:         1000,
		maxDelay:               defaultMaxDelay,
		maxMHsPerAdvertisement: defaultMaxMHsPerAdvertisement,
	}, ChainConfig{
		AdEntriesChunkSize: 0,
		ProviderId:         "",
		ProviderKey:        nil,
		ProviderAddrs:      nil,
		PublisherHttpAddrs: nil,
		Metadata:           nil,
	}, backend, announcer)

	for {
		// read from SQS
		msg := read()

		if msg.Publish {
			batcher.PublishCatalog(ctx, CatalogFromFoo(...))
		} else {
			batcher.RetractCatalog(ctx, CatalogFromFoo(...))
		}
	}

}
