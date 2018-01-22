package cdc_test

import (
  "github.com/ethereum/go-ethereum/ethdb"
  "github.com/ethereum/go-ethereum/ethdb/cdc"
  // "github.com/ethereum/go-ethereum/rlp"
  "testing"
  // "log"
  "bytes"
)

var test_values = []string{"", "a", "1251", "\x00123\x00"}

type MockBatch struct {
  KeyValues []cdc.KeyValue
}

func (batch *MockBatch) Put(key, value []byte) (error) {
  batch.KeyValues = append(batch.KeyValues, cdc.KeyValue{key, value})
  return nil
}

func (batch *MockBatch) ValueSize() int {
  size := 0
  for _, kv := range batch.KeyValues {
    size += len(kv.Value)
  }
  return size
}

func (batch *MockBatch) Write() error { return nil }

func (batch *MockBatch) GetKeyValues() []cdc.KeyValue { return batch.KeyValues }

func opsEqual(op1, op2 *cdc.Operation) (bool) {
  if op1.Op != op2.Op {
    return false
  }
  return bytes.Compare(op1.Data, op2.Data) == 0
}

func checkOperations(op *cdc.Operation, t *testing.T) {
  op2, err := cdc.OperationFromBytes(op.Bytes())
  if err != nil {
    t.Fatalf("Getting operation from bytes failed: %v", err)
  }
  if !opsEqual(op, op2) {
    t.Errorf("Expected ops to be equal")
  }
}

func TestEncodePutOperation(t *testing.T) {
  for _, v := range test_values {
    op, err := cdc.PutOperation([]byte(v), []byte(v))
    if err != nil {
      t.Fatalf("put failed: %v", err)
    }
    checkOperations(op, t)
    db, _ := ethdb.NewMemDatabase()
    err = op.Apply(db)
    if err != nil {
      t.Fatalf("operation.Apply failed: %v", err)
    }
    value, err := db.Get([]byte(v))
    if err != nil {
      t.Fatalf("Failed to get value '%v'", err)
    }
    if bytes.Compare(value, []byte(v)) != 0 {
      t.Errorf("Got unexpected value '%v' != '%v'", value, v)
    }
  }
}
func TestEncodeDeleteOperation(t *testing.T) {
  for _, v := range test_values {
    op, err := cdc.DeleteOperation([]byte(v))
    if err != nil {
      t.Fatalf("delete failed: %v", err)
    }
    checkOperations(op, t)
    db, _ := ethdb.NewMemDatabase()
    db.Put([]byte(v), []byte(v))
    err = op.Apply(db)
    if err != nil {
      t.Fatalf("operation.Apply failed: %v", err)
    }
    value, err := db.Get([]byte(v))
    if value != nil {
      t.Fatalf("Expected key to be deleted from database")
    }
  }
}
func TestEncodeWriteOperation(t *testing.T) {
  batch := &MockBatch{[]cdc.KeyValue{}}
  for _, v := range test_values {
    batch.Put([]byte(v), []byte(v))
  }
  op, err := cdc.WriteOperation(batch)
  if err != nil {
    t.Fatalf("batch write failed: %v", err)
  }
  checkOperations(op, t)
  db, _ := ethdb.NewMemDatabase()
  err = op.Apply(db)
  if err != nil {
    t.Fatalf("operation.Apply failed: %v", err)
  }
  for _, v := range test_values {
    value, err := db.Get([]byte(v))
    if err != nil {
      t.Errorf("error getting value from database: %v", err)
    }
    if bytes.Compare(value, []byte(v)) != 0 {
      t.Errorf("Got unexpected value '%v' != '%v'", value, v)
    }
  }
}

func TestEncodeGetOperation(t *testing.T) {
  for _, v := range test_values {
    op, err := cdc.GetOperation([]byte(v))
    if err != nil {
      t.Fatalf("read failed: %v", err)
    }
    checkOperations(op, t)
  }
}
func TestEncodeHasOperation(t *testing.T) {
  for _, v := range test_values {
    op, err := cdc.HasOperation([]byte(v))
    if err != nil {
      t.Fatalf("has failed: %v", err)
    }
    checkOperations(op, t)
  }
}
