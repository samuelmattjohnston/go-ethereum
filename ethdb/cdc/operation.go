package cdc

import (
  "fmt"
  "encoding/binary"
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
  OpBlockStart byte = 5
  OpBlockEnd byte = 6
)


type KeyValue struct {
  Key []byte
  Value []byte
}

func (kv *KeyValue) String() (string) {
  return fmt.Sprintf("KV{%#x(%v): %#x}", kv.Key, string(kv.Key), kv.Value)
}

type KVs []KeyValue

func (kvs KVs) String() string {
  output := "["
  for _, kv := range kvs {
    output += fmt.Sprintf("\n%v,", &kv)
  }
  output += "\n]"
  return output
}

type Operation struct {
  Op byte
  Data []byte
  Offset int64
  Topic string
}

func updateOffset(putter ethdb.Putter, op *Operation) {
  if op.Offset != 0 {
    buf := make([]byte, binary.MaxVarintLen64)
    binary.PutVarint(buf, op.Offset)
    putter.Put(
      []byte(fmt.Sprintf("cdc-log-%v-offset", op.Topic)),
      buf,
    )
  }
}
func (op *Operation) Apply(db ethdb.Database) error {
  switch op.Op {
  case OpPut:
    batch := db.NewBatch()
    kv := &KeyValue{}
    if err := rlp.DecodeBytes(op.Data, kv); err != nil { return err }
    if err := batch.Put(kv.Key, kv.Value); err != nil { return err }
    updateOffset(batch, op)
    if err := batch.Write(); err != nil { return err }
  case OpDelete:
    // For OpDelete, op.Data is the key to be deleted
    db.Delete(op.Data)
    updateOffset(db, op)
  case OpWrite:
    batch := db.NewBatch()
    var puts []KeyValue
    rlp.DecodeBytes(op.Data, &puts)
    for _, kv := range puts {
      if err := batch.Put(kv.Key, kv.Value); err != nil { return err }
    }
    updateOffset(batch, op)
    if err := batch.Write(); err != nil { return err }
  }
  return nil
}

func (op *Operation) Bytes() ([]byte) {
  data := []byte{op.Op}
  return append(data, op.Data...)
}

func (op *Operation) String() (string) {
  switch op.Op {
  case OpPut:
    kv := &KeyValue{}
    if err := rlp.DecodeBytes(op.Data, kv); err != nil { return err.Error() }
    return fmt.Sprintf("PUT: %v", kv)
  case OpDelete:
    return fmt.Sprintf("DEL: %v", string(op.Data))
  case OpWrite:
    var puts []KeyValue
    rlp.DecodeBytes(op.Data, &puts)
    return fmt.Sprintf("WRITE: %v", KVs(puts))
  case OpBlockStart:
    var blocknum uint64
    rlp.DecodeBytes(op.Data, &blocknum)
    return fmt.Sprintf("BLOCKSTART: %v", blocknum)
  case OpBlockEnd:
    var blocknum uint64
    rlp.DecodeBytes(op.Data, &blocknum)
    return fmt.Sprintf("BLOCKEND: %v", blocknum)
  }
  return "UNKNOWN"
}

func OperationFromBytes(data []byte, topic string, offset int64) (*Operation, error) {
  if len(data) == 0 {
    return nil, errors.New("OperationFromBytes requires a []byte of length > 0")
  }
  return &Operation{
    Op: data[0],
    Data: data[1:],
    Topic: topic,
    Offset: offset,
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
  return &Operation{OpDelete, key, 0, ""}, nil
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
  return &Operation{OpGet, key, 0, ""}, nil
}

func HasOperation(key []byte) (*Operation, error) {
  return &Operation{OpHas, key, 0, ""}, nil
}

func BlockStartOperation(blockNum uint64) (*Operation, error) {
  data, err := rlp.EncodeToBytes(blockNum)
  if err != nil { return nil, err }
  return &Operation{OpBlockStart, data, 0, ""}, nil
}

func BlockEndOperation(blockNum uint64) (*Operation, error) {
  data, err := rlp.EncodeToBytes(blockNum)
  if err != nil { return nil, err }
  return &Operation{OpBlockEnd, data, 0, ""}, nil
}
