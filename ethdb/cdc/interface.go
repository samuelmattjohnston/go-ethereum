package cdc

import (
  "github.com/ethereum/go-ethereum/ethdb"
)

type Batch interface {
  ethdb.Batch
  GetOperations() []BatchOperation
}

type LogProducer interface {
  Emit(*Operation) error
  Close()
}

type LogConsumer interface {
  Messages() <-chan *Operation
  Ready() <-chan struct{}
  Close()
}
