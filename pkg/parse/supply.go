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
	blockWorker    *supplyWorker
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
	t.blockWorker.periodicFileLogger.StartFileLogger(ctx)
	defer t.blockWorker.periodicFileLogger.StopFileLogger()
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

	var opts []storage.BadgerOption
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

	supplyWorker := newSupplyWorker(
		config.Data.ParseInterval,
		localStore,
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
			supplyWorker,
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
		blockWorker:    supplyWorker,
	}
}

// supplyWorker satisfies the storage.BlockWorker interface for the supply parser
type supplyWorker struct {
	interval           *configuration.DataParseInterval
	counter            *storage.CounterStorage
	seenFinalBlocks    map[int64]bool
	fetcher            *fetcher.Fetcher
	periodicFileLogger *PeriodicFileLogger
}

// AddingBlock is called by BlockStorage when adding a block to storage.
func (b *supplyWorker) AddingBlock(
	ctx context.Context,
	block *types.Block,
	transaction storage.DatabaseTransaction,
) (storage.CommitWorker, error) {
	for _, tx := range block.Transactions {
		if err := b.periodicFileLogger.Log(tx); err != nil {
			return nil, err
		}
	}
	return func(ctx context.Context) error {
		if _, ok := b.seenFinalBlocks[block.BlockIdentifier.Index]; ok {
			b.seenFinalBlocks[block.BlockIdentifier.Index] = true
		}
		return nil
	}, nil
}

// RemovingBlock is called by BlockStorage when removing a block from storage.
func (b *supplyWorker) RemovingBlock(
	ctx context.Context,
	block *types.Block,
	transaction storage.DatabaseTransaction,
) (storage.CommitWorker, error) {
	return func(ctx context.Context) error {
		return nil
	}, nil
}

// IsDone returns if the worker is done
func (b *supplyWorker) IsDone() bool {
	for _, v := range b.seenFinalBlocks {
		if v == false {
			return false
		}
	}
	return true
}

func newSupplyWorker(
	parseInterval *configuration.DataParseInterval,
	db storage.Database,
	fetcher *fetcher.Fetcher,
	maxSyncConcurrency int64,
) *supplyWorker {
	seenFinalBlocks := map[int64]bool{}
	startingVal := parseInterval.End.Index - maxSyncConcurrency
	if startingVal < 0 {
		startingVal = 0
	}
	for i := startingVal; i <= parseInterval.End.Index; i++ {
		seenFinalBlocks[i] = false
	}
	lgr := NewPeriodicFileLogger(
		fmt.Sprintf("./parse_output_<%v>.txt", time.Now().String()), // TODO: take as config input
		30*time.Second,
	)
	return &supplyWorker{
		interval:           parseInterval,
		counter:            storage.NewCounterStorage(db),
		seenFinalBlocks:    seenFinalBlocks,
		periodicFileLogger: lgr,
		fetcher:            fetcher,
	}
}
