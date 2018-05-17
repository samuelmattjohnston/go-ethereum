package replica

import (
  "context"
  "math/big"
  "github.com/ethereum/go-ethereum/eth/downloader"
  "github.com/ethereum/go-ethereum/ethdb"
  "github.com/ethereum/go-ethereum/event"
  "github.com/ethereum/go-ethereum/accounts"
  "github.com/ethereum/go-ethereum/common"
  "github.com/ethereum/go-ethereum/common/math"
  "github.com/ethereum/go-ethereum/rpc"
  "github.com/ethereum/go-ethereum/core"
  "github.com/ethereum/go-ethereum/core/types"
  "github.com/ethereum/go-ethereum/core/vm"
  "github.com/ethereum/go-ethereum/core/state"
  "github.com/ethereum/go-ethereum/params"
)

type ReplicaBackend struct {
  db ethdb.Database
  hc *core.HeaderChain
  chainConfig *params.ChainConfig
  bc *core.BlockChain
}

	// General Ethereum API
	// Block synchronization seems to happen at the downloader under normaly circumstances
func (backend *ReplicaBackend) Downloader() *downloader.Downloader {										// Seems to be used to get sync progress, cancel downloads {
  return nil
}
func (backend *ReplicaBackend) ProtocolVersion() int {																	// Static? {
  return 63
}
func (backend *ReplicaBackend) SuggestPrice(ctx context.Context) (*big.Int, error) {
  return new(big.Int), nil
}		// Use gas price oracle
func (backend *ReplicaBackend) ChainDb() ethdb.Database {															// Just return the database {
  return backend.db
}
func (backend *ReplicaBackend) EventMux() *event.TypeMux {															// Unused, afaict {
  return nil
}
func (backend *ReplicaBackend) AccountManager() *accounts.Manager {										// We don't want the read replicas to support accounts, so we'll want to minimize this {
  return accounts.NewManager()
}

	// BlockChain API

	// core.blockchain is the basis for most of these, but I think we may want to
	// reimplement much of that logic to just go straight to ChainDB

	// If we don't offer the private debug APIs, we don't need SetHead
func (backend *ReplicaBackend) SetHead(number uint64) {

}
	// This can probably lean on core.HeaderChain
func (backend *ReplicaBackend) HeaderByNumber(ctx context.Context, blockNr rpc.BlockNumber) (*types.Header, error) {
  if blockNr == rpc.LatestBlockNumber {
    latestHash := core.GetHeadHeaderHash(backend.db)
		return backend.hc.GetHeaderByHash(latestHash), nil
	}
	return backend.hc.GetHeaderByNumber(uint64(blockNr)), nil
} // Get block hash using HeaderByNumber, then get block with GetBlock() {

func (backend *ReplicaBackend) BlockByNumber(ctx context.Context, blockNr rpc.BlockNumber) (*types.Block, error) {
  if blockNr == rpc.LatestBlockNumber {
    latestHash := core.GetHeadBlockHash(backend.db)
		return backend.bc.GetBlockByHash(latestHash), nil
	}
	return backend.bc.GetBlockByNumber(uint64(blockNr)), nil
}
	// For StateAndHeaderByNumber, we'll need to construct a core.state object from
	// the state root for the specified block and the chaindb.
func (backend *ReplicaBackend) StateAndHeaderByNumber(ctx context.Context, blockNr rpc.BlockNumber) (*state.StateDB, *types.Header, error) {
  block, err := backend.BlockByNumber(ctx, blockNr)
  if err != nil {
    return nil, nil, err
  }
  stateDB, err := backend.bc.StateAt(block.Hash())
  return stateDB, block.Header(), err
}

	// This will need to rely on core.database_util.GetBlock instead of the core.blockchain version
func (backend *ReplicaBackend) GetBlock(ctx context.Context, blockHash common.Hash) (*types.Block, error) {
  return backend.bc.GetBlockByHash(blockHash), nil
}
	// Proxy core.GetBlockReceipts
func (backend *ReplicaBackend) GetReceipts(ctx context.Context, blockHash common.Hash) (types.Receipts, error) {
  return backend.bc.GetReceiptsByHash(blockHash), nil
}
	// This can probably lean on core.HeaderChain
func (backend *ReplicaBackend) GetTd(blockHash common.Hash) *big.Int {
  return backend.hc.GetTdByHash(blockHash)
}
	// Use core.NewEVMContext and vm.NewEVM - Will need custom ChainContext implementation
func (backend *ReplicaBackend) GetEVM(ctx context.Context, msg core.Message, state *state.StateDB, header *types.Header, vmCfg vm.Config) (*vm.EVM, func() error, error) {
  state.SetBalance(msg.From(), math.MaxBig256)
  vmError := func() error { return nil }

  context := core.NewEVMContext(msg, header, backend.bc, nil)
  return vm.NewEVM(context, state, backend.chainConfig, vmCfg), vmError, nil

}

	// I Don't think these are really need for RPC calls. Maybe stub them out?
func (backend *ReplicaBackend) SubscribeChainEvent(ch chan<- core.ChainEvent) event.Subscription {
  return nil
}
func (backend *ReplicaBackend) SubscribeChainHeadEvent(ch chan<- core.ChainHeadEvent) event.Subscription {
  return nil
}
func (backend *ReplicaBackend) SubscribeChainSideEvent(ch chan<- core.ChainSideEvent) event.Subscription {
  return nil
}

	// TxPool API

	// Perhaps we can put these on a Kafka queue back to the full node?
func (backend *ReplicaBackend) SendTx(ctx context.Context, signedTx *types.Transaction) error {
  return nil
}

	// Read replicas won't have the p2p functionality, so these will be noops

	// Return an empty transactions list
func (backend *ReplicaBackend) GetPoolTransactions() (types.Transactions, error) {
  return nil, nil
}

	// Return nil
func (backend *ReplicaBackend) GetPoolTransaction(txHash common.Hash) *types.Transaction {
  return nil
}

	// Generate core.state.managed_state object from current state, and get nonce from that
	// It won't account for have pending transactions
func (backend *ReplicaBackend) GetPoolNonce(ctx context.Context, addr common.Address) (uint64, error) {
  return 0, nil
}

	// 0,0
func (backend *ReplicaBackend) Stats() (pending int, queued int) {
  return 0, 0
}

	// Return empty maps
func (backend *ReplicaBackend) TxPoolContent() (map[common.Address]types.Transactions, map[common.Address]types.Transactions) {
  return make(map[common.Address]types.Transactions), make(map[common.Address]types.Transactions)
}

	// Not sure how to stub out subscriptions
func (backend *ReplicaBackend) SubscribeTxPreEvent(chan<- core.TxPreEvent) event.Subscription {
  return nil
}

func (backend *ReplicaBackend) ChainConfig() *params.ChainConfig {
  return backend.chainConfig
}

	// CurrentBlock needs to find the latest block number / hash from the DB, then
  // look that up using GetBlock() {

func (backend *ReplicaBackend) CurrentBlock() *types.Block {
  latestHash := core.GetHeadBlockHash(backend.db)
  return backend.bc.GetBlockByHash(latestHash)
}
