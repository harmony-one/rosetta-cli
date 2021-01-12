package cmd

import (
	"fmt"

	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/spf13/cobra"
)

var (
	parseSupplyCmd = &cobra.Command{
		Use:   "parse:supply",
		Short: "Parse all blocks from the given start to finish block identifiers",
		Run:   runParseSupplyCmd,
		Args:  cobra.ExactArgs(0),
	}
)

func runParseSupplyCmd(cmd *cobra.Command, args []string) {
	fmt.Printf(types.PrettyPrintStruct(Config.Data))
}
