package herald

func Example() {
	// awsCfg := aws.NewConfig()
	//
	// backend := NewS3Backend(awsCfg, "bucket", "/indexer/ingest/mainnet", keys)
	//
	// announcer := httpsender.New()
	//
	// batcher := StartCatalogBatcher(BatchConfig{
	// 	CountThreshold:         1000,
	// 	MaxDelay:               DefaultMaxDelay,
	// 	MaxMHsPerAdvertisement: DefaultMaxMHsPerAdvertisement,
	// }, ChainConfig{
	// 	AdEntriesChunkSize: 0,
	// 	PublisherId:        "",
	// 	PublisherKey:       nil,
	// 	ProviderAddrs:      nil,
	// 	PublisherHttpAddrs: nil,
	// 	Metadata:           nil,
	// }, backend, announcer)
	//
	// for {
	// 	// read from SQS
	// 	msg := read()
	//
	// 	if msg.Publish {
	// 		batcher.PublishCatalog(ctx, CatalogFromFoo(...))
	// 	} else {
	// 		batcher.RetractCatalog(ctx, CatalogFromFoo(...))
	// 	}
	// }
}
