package results

import (
	"math/big"

	"github.com/coinbase/rosetta-sdk-go/types"
)

// Supply is the result of the parse:supply command
type Supply struct {
	BlockID                  *types.BlockIdentifier `json:"block_identifier"`
	RewardsSoFar             *big.Int               `json:"approx_block_rewards_so_far"`      // Approximate due to async
	CirculatingSupplySoFar   *big.Int               `json:"approx_circulating_supply_so_far"` // Approximate due to async
	Rewards                  *big.Int               `json:"block_rewards"`
	TotalAmountDeducted      *big.Int               `json:"total_amount_deducted_from_accounts"`
	TotalAmountCredited      *big.Int               `json:"total_amount_credited_from_accounts"`
	NumOfContractsCreated    *big.Int               `json:"number_of_contracts_created"`
	NumOfTransactions        *big.Int               `json:"number_of_transactions"`
	NumOfAccounts            *big.Int               `json:"number_of_accounts"`
	AmountTransferred        *big.Int               `json:"amount_transferred"`
	CxReceived               *big.Int               `json:"cross_shard_amount_received"`
	CxSent                   *big.Int               `json:"cross_shard_amount_sent"`
	GasFees                  *big.Int               `json:"gas_fees"`
	NumOfUniqueAccountsSoFar *big.Int               `json:"approx_number_of_unique_accounts_so_far,omitempty"`
}
