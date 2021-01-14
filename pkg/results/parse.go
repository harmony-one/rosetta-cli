package results

import (
	"math/big"

	"github.com/coinbase/rosetta-sdk-go/types"
)

// Supply is the result of the parse:supply command
type Supply struct {
	BlockID                  *types.BlockIdentifier `json:"block_identifier"`
	RewardsSoFar             *big.Int               `json:"approx_block_rewards_so_far"`
	CirculatingSupplySoFar   *big.Int               `json:"approx_circulating_supply_so_far"`
	NumOfUniqueAccountsSoFar *big.Int               `json:"approx_number_of_unique_accounts_so_far,omitempty"`
	Rewards                  *big.Int               `json:"block_rewards"`
	NumOfTransactions        *big.Int               `json:"number_of_transactions"`
	NumOfAccounts            *big.Int               `json:"number_of_accounts"`
	AmountTransferred        *big.Int               `json:"amount_transferred"`
	GasFees                  *big.Int               `json:"gas_fees"`
}
