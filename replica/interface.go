package replica

import (
  "github.com/ethereum/go-ethereum/core"
  "github.com/ethereum/go-ethereum/core/types"
)

type TransactionProducer interface {
  Emit(marshall(*types.Transaction)) error
  RelayTransactions(*core.TxPool)
  Close()
}

type TransactionConsumer interface {
  Messages() <-chan *types.Transaction
  Close()
}
