package cdc

import (
  "github.com/ethereum/go-ethereum/rlp"
  "log"
)

type MockLogProducer struct {
  channel chan *Operation
}

var batches map[string][]BatchOperation


func (producer *MockLogProducer) Emit(data []byte) error {
  if batches == nil {
    batches = make(map[string][]BatchOperation)
  }
  var op *Operation
  var bop BatchOperation
  var err error
  if data[0] == 255 {
    bop, err = BatchOperationFromBytes(data, "", 0)
    if err != nil { return err }
    batch, ok := batches[string(bop.Batch[:])]
    if !ok {
      batch = []BatchOperation{}
    }
    batches[string(bop.Batch[:])] = append(batch, bop)
  } else {
    op, err = OperationFromBytes(data, "", 0)
    if op.Op == OpWrite {
      data, err := rlp.EncodeToBytes(batches[string(op.Data)])
      if err != nil {
        log.Printf("Failed to encode batch operation: %v", err)
      }
      delete(batches, string(op.Data))
      op.Data = append(op.Data, data...)
    }
    producer.channel <- op
  }
  return nil
}

func (producer *MockLogProducer) Close() {}

type MockLogConsumer struct {
  channel <-chan *Operation
}

func (consumer *MockLogConsumer) Messages() (<-chan *Operation) {
  return consumer.channel
}

func (consumer *MockLogConsumer) Ready() (<-chan struct{}) {
  channel := make(chan struct{})
  channel <- struct{}{}
  return channel
}

func (consumer *MockLogConsumer) Close() {}

func MockLogPair() (LogProducer, LogConsumer) {
  channel := make(chan *Operation, 2)
  return &MockLogProducer{channel}, &MockLogConsumer{channel}
}
