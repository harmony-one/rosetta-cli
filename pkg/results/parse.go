package results

import (
	"math/big"

	"github.com/coinbase/rosetta-sdk-go/types"
)

// Supply is the result of the parse:supply command
type Supply struct {
	BlockID                       *types.BlockIdentifier `json:"block_identifier"`
	RewardsAtBlock                *big.Int               `json:"block_rewards"`
	TotalCirculatingSupplyAtBlock *big.Int               `json:"total_circulating_supply"`
	NumOfTransactions             *big.Int               `json:"number_of_transactions"`
	AmountTransferred             *big.Int               `json:"amount_transferred"`
	GasFees                       *big.Int               `json:"gas_fees"`
}
