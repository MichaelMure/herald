package herald

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/ipni/go-libipni/announce/httpsender"
)

func Example() {
	awsCfg := aws.NewConfig()

	backend := newS3Backend(awsCfg, "bucket", "/indexer/ingest/mainnet", keys)

	announcer := httpsender.New()

	batcher := startCatalogBatcher(batchConfig{
		countThreshold:         1000,
		maxDelay:               defaultMaxDelay,
		maxMHsPerAdvertisement: defaultMaxMHsPerAdvertisement,
	}, chainConfig{
		adEntriesChunkSize: 0,
		providerId:         "",
		providerKey:        nil,
		providerAddrs:      nil,
		publisherHttpAddrs: nil,
		metadata:           nil,
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
