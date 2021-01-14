package parse

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"time"

	"github.com/coinbase/rosetta-cli/configuration"
	"github.com/coinbase/rosetta-cli/pkg/logger"
	"github.com/coinbase/rosetta-cli/pkg/results"
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
				fmt.Printf("finishing up...\n")
			}
		case <-tc.C:
			fmt.Printf(types.PrettyPrintStruct(t.blockWorker.LatestResult) + "\n")
			if t.blockWorker.IsDone() {
				fmt.Printf("--- DONE ---\n")
				return nil
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
		fetcher,
		config.Network,
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

const (
	// expendGasOperation is the operation type for gas on the Harmony network
	expendGasOperation = "Gas"
)

// supplyWorker satisfies the storage.BlockWorker interface for the supply parser
type supplyWorker struct {
	interval           *configuration.DataParseInterval
	seenFinalBlocks    map[int64]bool
	seenAccounts       map[string]interface{}
	rewardsSoFarCtr    *ThreadSafeCounter
	supplySoFarCtr     *ThreadSafeCounter
	fetcher            *fetcher.Fetcher
	network            *types.NetworkIdentifier
	periodicFileLogger *PeriodicFileLogger
	LatestResult       *results.Supply
}

// AddingBlock is called by BlockStorage when adding a block to storage.
func (b *supplyWorker) AddingBlock(
	ctx context.Context,
	block *types.Block,
	transaction storage.DatabaseTransaction,
) (storage.CommitWorker, error) {
	currResult := &results.Supply{
		BlockID:           block.BlockIdentifier,
		NumOfTransactions: big.NewInt(int64(len(block.Transactions))),
	}
	seenAccInBlock := map[string]interface{}{}
	gasFees, posAmtTxd, negAmtTxd := big.NewInt(0), big.NewInt(0), big.NewInt(0)

	for _, tx := range block.Transactions {
		if len(tx.Operations) == 0 {
			continue
		}
		for _, op := range tx.Operations {
			if _, ok := seenAccInBlock[op.Account.Address]; !ok {
				seenAccInBlock[op.Account.Address] = struct{}{}
			}
			if _, ok := b.seenAccounts[op.Account.Address]; !ok {
				b.seenAccounts[op.Account.Address] = struct{}{}
			}
			amount, err := types.AmountValue(op.Amount)
			if err != nil {
				return nil, err
			}
			if amount.Sign() == -1 {
				negAmtTxd = new(big.Int).Add(new(big.Int).Abs(amount), negAmtTxd)
			} else if amount.Sign() == 1 {
				posAmtTxd = new(big.Int).Add(amount, posAmtTxd)
			}
			if op.Type == expendGasOperation {
				gasFees = new(big.Int).Add(new(big.Int).Abs(amount), gasFees)
			}
		}
	}

	currResult.Rewards = new(big.Int).Sub(posAmtTxd, new(big.Int).Add(negAmtTxd, gasFees))
	b.rewardsSoFarCtr.Add(currResult.Rewards)
	b.supplySoFarCtr.Add(posAmtTxd)
	currResult.GasFees = gasFees
	currResult.AmountTransferred = negAmtTxd
	currResult.NumOfAccounts = big.NewInt(int64(len(seenAccInBlock)))
	currResult.NumOfUniqueAccountsSoFar = big.NewInt(int64(len(b.seenAccounts)))
	currResult.RewardsSoFar = b.rewardsSoFarCtr.Count
	currResult.CirculatingSupplySoFar = b.supplySoFarCtr.Count
	if err := b.periodicFileLogger.Log(currResult); err != nil {
		return nil, err
	}

	return func(ctx context.Context) error {
		if _, ok := b.seenFinalBlocks[block.BlockIdentifier.Index]; ok {
			b.seenFinalBlocks[block.BlockIdentifier.Index] = true
		}
		b.LatestResult = currResult
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
	fetcher *fetcher.Fetcher,
	network *types.NetworkIdentifier,
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
	return &supplyWorker{
		interval:        parseInterval,
		seenFinalBlocks: seenFinalBlocks,
		seenAccounts:    map[string]interface{}{},
		rewardsSoFarCtr: NewAtomicCounter(context.Background()),
		supplySoFarCtr:  NewAtomicCounter(context.Background()),
		fetcher:         fetcher,
		network:         network,
		periodicFileLogger: NewPeriodicFileLogger(
			fmt.Sprintf("./parse_output_<%v>", time.Now().String()), // TODO: take as config input
			2*time.Second,
		),
	}
}