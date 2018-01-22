package cdc

import (
  "github.com/ethereum/go-ethereum/ethdb"
  "github.com/ethereum/go-ethereum/rlp"
  "errors"
)

const (
  OpPut byte = 0
  OpDelete byte = 1
  OpWrite byte = 2
  OpGet byte = 3
  OpHas byte = 4
)


type KeyValue struct {
  Key []byte
  Value []byte
}


type Operation struct {
  Op byte
  Data []byte
}

func (op *Operation) Apply(db ethdb.Database) error {
  switch op.Op {
  case OpPut:
    kv := &KeyValue{}
    if err := rlp.DecodeBytes(op.Data, kv); err != nil { return err }
    if err := db.Put(kv.Key, kv.Value); err != nil { return err }
  case OpDelete:
    // For OpDelete, op.Data is the key to be deleted
    db.Delete(op.Data)
  case OpWrite:
    batch := db.NewBatch()
    var puts []KeyValue
    rlp.DecodeBytes(op.Data, &puts)
    for _, kv := range puts {
      if err := batch.Put(kv.Key, kv.Value); err != nil { return err }
    }
    if err := batch.Write(); err != nil { return err }
  }
  return nil
}

func (op *Operation) Bytes() ([]byte) {
  data := []byte{op.Op}
  return append(data, op.Data...)
}

func OperationFromBytes(data []byte) (*Operation, error) {
  if len(data) == 0 {
    return nil, errors.New("OperationFromBytes requires a []byte of length > 0")
  }
  return &Operation{
    Op: data[0],
    Data: data[1:],
  }, nil
}

func PutOperation(key, value []byte) (*Operation, error) {
  op := &Operation{}
  op.Op = OpPut
  data, err := rlp.EncodeToBytes(KeyValue{key, value})
  if err != nil { return nil, err }
  op.Data = data
  return op, nil
}

func DeleteOperation(key []byte) (*Operation, error) {
  return &Operation{OpDelete, key}, nil
}

func WriteOperation(batch Batch) (*Operation, error) {
  op := &Operation{}
  op.Op = OpWrite
  data, err := rlp.EncodeToBytes(batch.GetKeyValues())
  if err != nil { return nil, err }
  op.Data = data
  return op, nil
}

func GetOperation(key []byte) (*Operation, error) {
  return &Operation{OpGet, key}, nil
}

func HasOperation(key []byte) (*Operation, error) {
  return &Operation{OpHas, key}, nil
}
