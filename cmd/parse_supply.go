package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"

	"github.com/coinbase/rosetta-cli/pkg/parse"
	"github.com/coinbase/rosetta-cli/pkg/results"
	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/utils"
)

var (
	parseSupplyCmd = &cobra.Command{
		Use:   "parse:supply",
		Short: "Parse all blocks from the given start to finish block identifiers",
		RunE:  runParseSupplyCmd,
		Args:  cobra.ExactArgs(0),
	}
)

func runParseSupplyCmd(cmd *cobra.Command, args []string) error {
	ensureDataDirectoryExists()
	ctx, cancel := context.WithCancel(context.Background())

	fetch := fetcher.New(
		Config.OnlineURL,
		fetcher.WithMaxConnections(Config.MaxOnlineConnections),
		fetcher.WithRetryElapsedTime(time.Duration(Config.RetryElapsedTime)*time.Second),
		fetcher.WithTimeout(time.Duration(Config.HTTPTimeout)*time.Second),
		fetcher.WithMaxRetries(Config.MaxRetries),
	)

	_, _, fetchErr := fetch.InitializeAsserter(ctx, Config.Network)
	if fetchErr != nil {
		cancel()
		return results.ExitData(
			Config,
			nil,
			nil,
			fmt.Errorf("%w: unable to initialize asserter", fetchErr.Err),
			"",
			"",
		)
	}

	networkStatus, err := utils.CheckNetworkSupported(ctx, Config.Network, fetch)
	if err != nil {
		cancel()
		return results.ExitData(
			Config,
			nil,
			nil,
			fmt.Errorf("%w: unable to confirm network", err),
			"",
			"",
		)
	}

	supplyParser := parse.InitializeSupplyParser(
		ctx,
		Config,
		Config.Network,
		fetch,
		cancel,
		networkStatus.GenesisBlockIdentifier,
		&SignalReceived,
	)

	defer supplyParser.CloseDatabase(ctx)

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return supplyParser.StartSyncing(ctx)
	})
	return supplyParser.WatchEndConditions(ctx)
}
