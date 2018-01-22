package cdc_test

import (
  "github.com/ethereum/go-ethereum/ethdb"
  "github.com/ethereum/go-ethereum/ethdb/cdc"
  "github.com/ethereum/go-ethereum/rlp"
  "testing"
  "bytes"
)

func getTestWrapper() (ethdb.Database, cdc.LogConsumer, cdc.LogConsumer) {
  writeProducer, writeConsumer := cdc.MockLogPair()
  readProducer, readConsumer := cdc.MockLogPair()
  db, _ := ethdb.NewMemDatabase()
  return cdc.NewDBWrapper(db, writeProducer, readProducer), writeConsumer, readConsumer
}

func TestPut(t *testing.T) {
  db, writeStream, _ := getTestWrapper()
  defer db.Close()
  err := db.Put([]byte("hello"), []byte("world"))
  if err != nil {
    t.Fatalf(err.Error())
  }
  select {
  case op := <-writeStream.Messages():
    if op.Op != cdc.OpPut {
      t.Errorf("Got unexpected operation type: %v", op.Op)
    }
    kv := &cdc.KeyValue{}
    rlp.DecodeBytes(op.Data, kv)
    if bytes.Compare(kv.Key, []byte("hello")) != 0 || bytes.Compare(kv.Value, []byte("world")) != 0 {
      t.Errorf("Unexpected Key Pair: %v: %v", kv.Key, kv.Value)
    }
  default:
    t.Fatalf("No message available on stream")
  }
}

func TestGet(t *testing.T) {
  db, _, readStream := getTestWrapper()
  defer db.Close()
  err := db.Put([]byte("hello"), []byte("world"))
  if err != nil {
    t.Fatalf(err.Error())
  }
  val, err := db.Get([]byte("hello"))
  if err != nil {
    t.Fatalf(err.Error())
  }
  if bytes.Compare(val, []byte("world")) != 0 {
    t.Errorf("Unexpected value from db get: %v", val)
  }
  select {
  case op := <-readStream.Messages():
    if op.Op != cdc.OpGet {
      t.Errorf("Got unexpected operation type: %v", op.Op)
    }
    if bytes.Compare(op.Data, []byte("hello")) != 0 {
      t.Errorf("Unexpected Key: %v", op.Data)
    }
  default:
    t.Fatalf("No message available on stream")
  }
}

func TestHas(t *testing.T) {
  db, _, readStream := getTestWrapper()
  defer db.Close()
  err := db.Put([]byte("hello"), []byte("world"))
  if err != nil {
    t.Fatalf(err.Error())
  }
  val, err := db.Has([]byte("hello"))
  if err != nil {
    t.Fatalf(err.Error())
  }
  if !val {
    t.Errorf("Unexpected value from db get: %v", val)
  }
  val, err = db.Has([]byte("world"))
  if err != nil {
    t.Fatalf(err.Error())
  }
  if val {
    t.Errorf("Unexpected value from db get: %v", val)
  }
  select {
  case op := <-readStream.Messages():
    if op.Op != cdc.OpHas {
      t.Errorf("Got unexpected operation type: %v", op.Op)
    }
    if bytes.Compare(op.Data, []byte("hello")) != 0 {
      t.Errorf("Unexpected Key: %v", op.Data)
    }
  default:
    t.Fatalf("No message available on stream")
  }
  select {
  case op := <-readStream.Messages():
    if op.Op != cdc.OpHas {
      t.Errorf("Got unexpected operation type: %v", op.Op)
    }
    if bytes.Compare(op.Data, []byte("world")) != 0 {
      t.Errorf("Unexpected Key: %v", op.Data)
    }
  default:
    t.Fatalf("No message available on stream")
  }
}

func TestDelete(t *testing.T) {
  db, writeStream, _ := getTestWrapper()
  defer db.Close()
  err := db.Put([]byte("hello"), []byte("world"))
  if err != nil {
    t.Fatalf(err.Error())
  }
  select {
  case _ = <-writeStream.Messages():
  default:
    t.Errorf("Expected put messsage on stream")
  }
  err = db.Delete([]byte("hello"))
  if err != nil {
    t.Fatalf(err.Error())
  }
  select {
  case op := <-writeStream.Messages():
    if op.Op != cdc.OpDelete {
      t.Errorf("Got unexpected operation type: %v", op.Op)
    }
    if bytes.Compare(op.Data, []byte("hello")) != 0 {
      t.Errorf("Unexpected Key: %v", op.Data)
    }
  default:
    t.Fatalf("No message available on stream")
  }
  if val, _ := db.Has([]byte("hello")); val {
    t.Errorf("Key unexpectedly found in %s database")
  }
}

func TestBatchWrapper(t *testing.T) {
  db, writeStream, _ := getTestWrapper()
  defer db.Close()
  batch := db.NewBatch()
  batch.Put([]byte("hello"), []byte("world"))
  select {
  case _ = <-writeStream.Messages():
    t.Errorf("No messages expected yet")
  default:
  }
  if size := batch.ValueSize(); size != 5 {
    t.Errorf("Unexpected value size %v", size)
  }
  batch.Put([]byte("goodbye"), []byte("world"))
  select {
  case _ = <-writeStream.Messages():
    t.Errorf("No messages expected yet")
  default:
  }
  if size := batch.ValueSize(); size != 10 {
    t.Errorf("Unexpected value size %v", size)
  }
  batch.Write()
  select {
  case op := <-writeStream.Messages():
    if op.Op != cdc.OpWrite {
      t.Errorf("Got unexpected operation type: %v", op.Op)
    }
    kvs := []cdc.KeyValue{}
    rlp.DecodeBytes(op.Data, &kvs)
    if bytes.Compare(kvs[0].Key, []byte("hello")) != 0 || bytes.Compare(kvs[0].Value, []byte("world")) != 0 {
      t.Errorf("Unexpected Key Value Pair: %v, %v", kvs[0].Key, kvs[0].Value)
    }
    if bytes.Compare(kvs[1].Key, []byte("goodbye")) != 0 || bytes.Compare(kvs[1].Value, []byte("world")) != 0 {
      t.Errorf("Unexpected Key Value Pair: %v, %v", kvs[1].Key, kvs[1].Value)
    }
  default:
    t.Errorf("Expected batch write")
  }


}
