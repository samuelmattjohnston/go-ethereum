package cdc

import (
  "github.com/ethereum/go-ethereum/ethdb"
)

type Batch interface {
  ethdb.Batch
  GetKeyValues() []KeyValue
}

type LogProducer interface {
  Emit(*Operation) error
  Close()
}

type LogConsumer interface {
  Messages() <-chan *Operation
  Close()
}
