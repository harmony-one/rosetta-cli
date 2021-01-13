package parse

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/coinbase/rosetta-cli/configuration"
	"github.com/coinbase/rosetta-cli/pkg/logger"
	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/statefulsyncer"
	"github.com/coinbase/rosetta-sdk-go/storage"
	"github.com/coinbase/rosetta-sdk-go/syncer"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"
)

const (
	// dataCmdName is used as the prefix on the data directory
	// for all data saved using this command.
	dataCmdName = "parse-supply"

	// DoneCheckPeriod ..
	DoneCheckPeriod = 1 * time.Second

	debug = false
)

// DataTester coordinates the `parse:supply` run.
type SupplyParser struct {
	network        *types.NetworkIdentifier
	database       storage.Database
	config         *configuration.Configuration
	syncer         *statefulsyncer.StatefulSyncer
	logger         *logger.Logger
	blockStorage   *storage.BlockStorage
	counterStorage *storage.CounterStorage
	fetcher        *fetcher.Fetcher
	signalReceived *bool
	genesisBlock   *types.BlockIdentifier
	cancel         context.CancelFunc
	blockWorker    *blockWorker
}

// CloseDatabase closes the database used by DataTester.
func (t *SupplyParser) CloseDatabase(ctx context.Context) {
	if err := t.database.Close(ctx); err != nil {
		log.Fatalf("%s: error closing database", err.Error())
	}
}

// StartSyncing syncs from startIndex to endIndex.
func (t *SupplyParser) StartSyncing(
	ctx context.Context,
) error {
	// TODO: verify that blocks exist on node before starting...
	return t.syncer.Sync(
		ctx, t.config.Data.ParseInterval.Start.Index, t.config.Data.ParseInterval.End.Index,
	)
}

// WatchEndConditions starts go routines to watch the end conditions
func (t *SupplyParser) WatchEndConditions(
	ctx context.Context,
) error {
	tc := time.NewTicker(DoneCheckPeriod)
	defer tc.Stop()
	for {
		select {
		case <-ctx.Done():
			if t.blockWorker.IsDone() {
				fmt.Printf("--- DONE ---\n")
				return nil
			} else {
				fmt.Printf("finishing up...\n") // TODO: print final results...
			}
		case <-tc.C:
			if t.blockWorker.IsDone() {
				fmt.Printf("--- DONE ---\n") // TODO: print final results...
				return nil
			} else {
				// TODO: print current progress for recovery reasons...
			}
		}
	}
}

func InitializeSupplyParser(
	ctx context.Context,
	config *configuration.Configuration,
	network *types.NetworkIdentifier,
	fetcher *fetcher.Fetcher,
	cancel context.CancelFunc,
	genesisBlock *types.BlockIdentifier,
	signalReceived *bool,
) *SupplyParser {
	dataPath, err := utils.CreateCommandPath(config.DataDirectory, dataCmdName, network)
	if err != nil {
		log.Fatalf("%s: cannot create command path", err.Error())
	}

	opts := []storage.BadgerOption{}
	if config.CompressionDisabled {
		opts = append(opts, storage.WithoutCompression())
	}
	if config.MemoryLimitDisabled {
		opts = append(opts, storage.WithCustomSettings(storage.PerformanceBadgerOptions(dataPath)))
	}

	localStore, err := storage.NewBadgerStorage(ctx, dataPath, opts...)
	if err != nil {
		log.Fatalf("%s: unable to initialize database", err.Error())
	}

	counterStorage := storage.NewCounterStorage(localStore)
	blockStorage := storage.NewBlockStorage(localStore)

	lgr := logger.NewLogger(
		dataPath,
		config.Data.LogBlocks,
		config.Data.LogTransactions,
		config.Data.LogBalanceChanges,
		config.Data.LogReconciliations,
	)

	worker := newBlockWorker(
		config.Data.ParseInterval,
		fetcher,
		config.MaxSyncConcurrency,
	)

	stateSync := statefulsyncer.New(
		ctx,
		network,
		fetcher,
		blockStorage,
		counterStorage,
		lgr,
		cancel,
		[]storage.BlockWorker{
			worker,
		},
		syncer.DefaultCacheSize,
		config.MaxSyncConcurrency,
		config.MaxReorgDepth,
	)

	return &SupplyParser{
		network:        network,
		database:       localStore,
		config:         config,
		syncer:         stateSync,
		cancel:         cancel,
		logger:         lgr,
		blockStorage:   blockStorage,
		counterStorage: counterStorage,
		fetcher:        fetcher,
		signalReceived: signalReceived,
		genesisBlock:   genesisBlock,
		blockWorker:    worker,
	}
}

// blockWorker for the supply parser's block worker
type blockWorker struct {
	parseOutputFile string
	interval        *configuration.DataParseInterval
	seenFinalBlocks map[int64]bool
	fetcher         *fetcher.Fetcher
}

// AddingBlock is called by BlockStorage when adding a block to storage.
func (b *blockWorker) AddingBlock(
	ctx context.Context,
	block *types.Block,
	transaction storage.DatabaseTransaction,
) (storage.CommitWorker, error) {
	return func(ctx context.Context) error {
		if _, ok := b.seenFinalBlocks[block.BlockIdentifier.Index]; ok {
			b.seenFinalBlocks[block.BlockIdentifier.Index] = true
		}
		return nil
	}, nil
}

// RemovingBlock is called by BlockStorage when removing a block from storage.
func (b *blockWorker) RemovingBlock(
	ctx context.Context,
	block *types.Block,
	transaction storage.DatabaseTransaction,
) (storage.CommitWorker, error) {
	return func(ctx context.Context) error {
		return nil
	}, nil
}

// IsDone returns if the worker is done
func (b *blockWorker) IsDone() bool {
	for _, v := range b.seenFinalBlocks {
		if v == false {
			return false
		}
	}
	return true
}

func newBlockWorker(
	parseInterval *configuration.DataParseInterval,
	fetcher *fetcher.Fetcher,
	maxSyncConcurrency int64,
) *blockWorker {
	seenFinalBlocks := map[int64]bool{}
	startingVal := parseInterval.End.Index - maxSyncConcurrency
	if startingVal < 0 {
		startingVal = 0
	}
	for i := startingVal; i <= parseInterval.End.Index; i++ {
		seenFinalBlocks[i] = false
	}
	return &blockWorker{
		interval:        parseInterval,
		parseOutputFile: "./parse_result.txt",
		seenFinalBlocks: seenFinalBlocks,
		fetcher:         fetcher,
	}
}
