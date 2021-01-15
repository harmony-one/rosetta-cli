package parse

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"os"
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
	"github.com/fatih/color"
)

const (
	// dataCmdName is used as the prefix on the data directory
	// for all data saved using this command.
	dataCmdName = "parse-supply"

	// DoneCheckPeriod ..
	DoneCheckPeriod = 10 * time.Second
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

// StartSyncing syncs from startIndex to endIndex, so long as the blocks are finalized.
func (t *SupplyParser) StartSyncing(
	ctx context.Context,
) error {
	_, err := t.fetcher.BlockRetry(ctx, t.network, &types.PartialBlockIdentifier{
		Index: &t.config.Data.ParseInterval.Start.Index,
		Hash:  &t.config.Data.ParseInterval.Start.Hash,
	})
	if err != nil {
		color.Red("start block (%v) not found", t.config.Data.ParseInterval.Start.Hash)
		return err.Err
	}
	_, err = t.fetcher.BlockRetry(ctx, t.network, &types.PartialBlockIdentifier{
		Index: &t.config.Data.ParseInterval.End.Index,
		Hash:  &t.config.Data.ParseInterval.End.Hash,
	})
	if err != nil {
		color.Red("end block (%v) not found", t.config.Data.ParseInterval.End.Hash)
		return err.Err
	}

	return t.syncer.Sync(
		ctx, t.config.Data.ParseInterval.Start.Index, t.config.Data.ParseInterval.End.Index,
	)
}

// StartPruning attempts to prune block storage
// every 10 seconds.
func (t *SupplyParser) StartPruning(
	ctx context.Context,
) error {
	return t.syncer.Prune(ctx, t)
}

func (t *SupplyParser) PruneableIndex(ctx context.Context, headIndex int64) (int64, error) {
	if t.blockWorker.LatestResult == nil || t.blockWorker.LatestResult.BlockID == nil {
		return 0, nil
	}
	safestBlockToRemove := t.blockWorker.LatestResult.BlockID.Index - t.config.MaxSyncConcurrency
	if safestBlockToRemove < 0 {
		safestBlockToRemove = 0
	}
	return safestBlockToRemove, nil
}

// WatchEndConditions starts go routines to watch the end conditions
func (t *SupplyParser) WatchEndConditions(
	ctx context.Context,
) error {
	t.blockWorker.periodicFileLogger.StartFileLogger(context.Background())
	defer t.blockWorker.periodicFileLogger.StopFileLogger()
	t.blockWorker.periodicallySaveUniqueAccounts( // TODO: consider disabling this if causing problem...
		context.Background(),
		30*time.Minute,
		fmt.Sprintf("./parse_last_seen_accounts_<%v>", time.Now().String()), // TODO: take as config input
	)
	tc := time.NewTicker(DoneCheckPeriod)
	defer tc.Stop()

	done := func() error {
		fmt.Printf("Total (Final) Block Rewards: %v\n", t.blockWorker.rewardsSoFarCtr.GetCount())
		fmt.Printf("Total (Final) Circulating Supply: %v\n", t.blockWorker.supplySoFarCtr.GetCount())
		fmt.Printf("--- DONE ---\n")
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			fmt.Printf(types.PrettyPrintStruct(t.blockWorker.LatestResult) + "\n")
			if t.blockWorker.IsDone() {
				return done()
			}
			return ctx.Err()
		case <-tc.C:
			fmt.Printf(types.PrettyPrintStruct(t.blockWorker.LatestResult) + "\n")
			if t.blockWorker.IsDone() {
				return done()
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
	// WARNING: following constants are dependant on the Harmony Rosetta Implementation.
	// expendGasOperation is the operation type for gas on the Harmony network
	expendGasOperation = "Gas"
	// crossShardTransferOperation is the operation type for cx on the Harmony network
	crossShardTransferOperation = "NativeCrossShardTransfer"
	// preStakingBlockRewardsOperation is the operation type for all block rewards in the
	// pre staking ear on the Harmony network.
	preStakingBlockRewardsOperation = "PreStakingBlockReward"
	// collectRewardsOperation is the operation type for the block rewards in the staking era
	collectRewardsOperation = "CollectRewards"
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
	cxReceived, cxSent := big.NewInt(0), big.NewInt(0)
	rewards, amountTransferred := big.NewInt(0), big.NewInt(0)

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
				switch op.Type {
				case crossShardTransferOperation:
					cxSent = new(big.Int).Add(new(big.Int).Abs(amount), cxSent)
				case expendGasOperation:
					gasFees = new(big.Int).Add(new(big.Int).Abs(amount), gasFees)
				default:
					amountTransferred = new(big.Int).Add(new(big.Int).Abs(amount), amountTransferred)
				}
			} else if amount.Sign() == 1 {
				posAmtTxd = new(big.Int).Add(amount, posAmtTxd)
				switch op.Type {
				case crossShardTransferOperation:
					cxReceived = new(big.Int).Add(new(big.Int).Abs(amount), cxReceived)
				case preStakingBlockRewardsOperation:
					rewards = new(big.Int).Add(new(big.Int).Abs(amount), rewards)
				case collectRewardsOperation:
					rewards = new(big.Int).Add(new(big.Int).Abs(amount), rewards)
				}
			}
		}
	}

	b.rewardsSoFarCtr.Add(rewards)
	b.supplySoFarCtr.Add(new(big.Int).Sub(posAmtTxd, negAmtTxd))
	currResult.TotalAmountCredited = posAmtTxd
	currResult.TotalAmountDeducted = negAmtTxd
	currResult.Rewards = rewards
	currResult.AmountTransferred = amountTransferred
	currResult.CxSent = cxSent
	currResult.CxReceived = cxReceived
	currResult.GasFees = gasFees
	currResult.NumOfAccounts = big.NewInt(int64(len(seenAccInBlock)))
	currResult.NumOfUniqueAccountsSoFar = big.NewInt(int64(len(b.seenAccounts)))
	currResult.RewardsSoFar = b.rewardsSoFarCtr.GetCount()
	currResult.CirculatingSupplySoFar = b.supplySoFarCtr.GetCount()
	if err := b.periodicFileLogger.Log(currResult); err != nil {
		return nil, err
	}

	return func(ctx context.Context) error {
		if _, ok := b.seenFinalBlocks[block.BlockIdentifier.Index]; ok {
			b.seenFinalBlocks[block.BlockIdentifier.Index] = true
		}
		if b.LatestResult == nil || b.LatestResult.BlockID == nil ||
			currResult.BlockID.Index > b.LatestResult.BlockID.Index {
			b.LatestResult = currResult
		}
		transaction.Discard(ctx)
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

func (b *supplyWorker) periodicallySaveUniqueAccounts(
	ctx context.Context,
	interval time.Duration,
	fileName string,
) {
	write := func() {
		color.Cyan("Saving Uniquely Seen Accounts")
		if err := os.Remove(fileName); err != nil && !os.IsNotExist(err) {
			color.Red(err.Error())
			return
		}
		f, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			color.Red(err.Error())
			return
		}
		for k := range b.seenAccounts {
			if _, err := f.WriteString(k + PeriodicFileLoggerDelimiter); err != nil {
				color.Red(err.Error())
				break
			}
		}
		if b.LatestResult.BlockID != nil {
			if _, err := f.WriteString(fmt.Sprintf("%v", b.LatestResult.BlockID.Index)); err != nil {
				color.Red(err.Error())
			}
		}
		if err := f.Close(); err != nil {
			color.Red("trouble closing file: %v", err.Error())
		}
	}

	tc := time.NewTicker(interval)
	defer tc.Stop()
	for {
		select {
		case <-ctx.Done():
			write()
			return
		case <-tc.C:
			write()
		}
	}
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
		rewardsSoFarCtr: NewThreadSafeCounter(),
		supplySoFarCtr:  NewThreadSafeCounter(),
		fetcher:         fetcher,
		network:         network,
		LatestResult:    &results.Supply{},
		periodicFileLogger: NewPeriodicFileLogger(
			fmt.Sprintf("./parse_output_<%v>", time.Now().String()), // TODO: take as config input
			20*time.Second,
		),
	}
}
