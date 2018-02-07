// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Package ethapi implements the general Ethereum API functions.
package ethapi

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/eth/downloader"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rpc"
)

// TODO: Figure out where in the node API backends are created

// Backend interface provides the common API services (that are provided by
// both full and light clients) with access to necessary functions.
type Backend interface {
	// General Ethereum API
	// Block synchronization seems to happen at the downloader under normaly circumstances
	Downloader() *downloader.Downloader										// Seems to be used to get sync progress, cancel downloads
	ProtocolVersion() int																	// Static?
	SuggestPrice(ctx context.Context) (*big.Int, error)		// Use gas price oracle
	ChainDb() ethdb.Database															// Just return the database
	EventMux() *event.TypeMux															// Unused, afaict
	AccountManager() *accounts.Manager										// We don't want the read replicas to support accounts, so we'll want to minimize this

	// BlockChain API

	// core.blockchain is the basis for most of these, but I think we may want to
	// reimplement much of that logic to just go straight to ChainDB

	// If we don't offer the private debug APIs, we don't need SetHead
	SetHead(number uint64)
	// This can probably lean on core.HeaderChain
	HeaderByNumber(ctx context.Context, blockNr rpc.BlockNumber) (*types.Header, error)
	// Get block hash using HeaderByNumber, then get block with GetBlock()
	BlockByNumber(ctx context.Context, blockNr rpc.BlockNumber) (*types.Block, error)
	// For StateAndHeaderByNumber, we'll need to construct a core.state object from
	// the state root for the specified block and the chaindb.
	StateAndHeaderByNumber(ctx context.Context, blockNr rpc.BlockNumber) (*state.StateDB, *types.Header, error)

	// This will need to rely on core.database_util.GetBlock instead of the core.blockchain version
	GetBlock(ctx context.Context, blockHash common.Hash) (*types.Block, error)
	// Proxy core.GetBlockReceipts
	GetReceipts(ctx context.Context, blockHash common.Hash) (types.Receipts, error)
	// This can probably lean on core.HeaderChain
	GetTd(blockHash common.Hash) *big.Int
	// Use core.NewEVMContext and vm.NewEVM - Will need custom ChainContext implementation
	GetEVM(ctx context.Context, msg core.Message, state *state.StateDB, header *types.Header, vmCfg vm.Config) (*vm.EVM, func() error, error)

	// I Don't think these are really need for RPC calls. Maybe stub them out?
	SubscribeChainEvent(ch chan<- core.ChainEvent) event.Subscription
	SubscribeChainHeadEvent(ch chan<- core.ChainHeadEvent) event.Subscription
	SubscribeChainSideEvent(ch chan<- core.ChainSideEvent) event.Subscription

	// TxPool API

	// Perhaps we can put these on a Kafka queue back to the full node?
	SendTx(ctx context.Context, signedTx *types.Transaction) error

	// Read replicas won't have the p2p functionality, so these will be noops

	// Return an empty transactions list
	GetPoolTransactions() (types.Transactions, error)

	// Return nil
	GetPoolTransaction(txHash common.Hash) *types.Transaction

	// Generate core.state.managed_state object from current state, and get nonce from that
	// It won't account for have pending transactions
	GetPoolNonce(ctx context.Context, addr common.Address) (uint64, error)

	// 0,0
	Stats() (pending int, queued int)

	// Return empty maps
	TxPoolContent() (map[common.Address]types.Transactions, map[common.Address]types.Transactions)

	// Not sure how to stub out subscriptions
	SubscribeTxPreEvent(chan<- core.TxPreEvent) event.Subscription

	ChainConfig() *params.ChainConfig

	// CurrentBlock needs to find the latest block number / hash from the DB, then
	// look that up using GetBlock()
	CurrentBlock() *types.Block
}

func GetAPIs(apiBackend Backend) []rpc.API {
	nonceLock := new(AddrLocker)
	return []rpc.API{
		{
			Namespace: "eth",
			Version:   "1.0",
			Service:   NewPublicEthereumAPI(apiBackend),
			Public:    true,
		}, {
			Namespace: "eth",
			Version:   "1.0",
			Service:   NewPublicBlockChainAPI(apiBackend),
			Public:    true,
		}, {
			Namespace: "eth",
			Version:   "1.0",
			Service:   NewPublicTransactionPoolAPI(apiBackend, nonceLock),
			Public:    true,
		}, {
			Namespace: "txpool",
			Version:   "1.0",
			Service:   NewPublicTxPoolAPI(apiBackend),
			Public:    true,
		}, {
			Namespace: "debug",
			Version:   "1.0",
			Service:   NewPublicDebugAPI(apiBackend),
			Public:    true,
		}, {
			Namespace: "debug",
			Version:   "1.0",
			Service:   NewPrivateDebugAPI(apiBackend),
		}, {
			Namespace: "eth",
			Version:   "1.0",
			Service:   NewPublicAccountAPI(apiBackend.AccountManager()),
			Public:    true,
		}, {
			Namespace: "personal",
			Version:   "1.0",
			Service:   NewPrivateAccountAPI(apiBackend, nonceLock),
			Public:    false,
		},
	}
}
