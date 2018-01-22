package cdc
import (
  "github.com/ethereum/go-ethereum/ethdb"
)

type BatchWrapper struct {
  batch ethdb.Batch
  writeStream LogProducer
  keyValues []KeyValue
}

func (batch *BatchWrapper) Put(key, value []byte) (error) {
  if batch.writeStream != nil {
    batch.keyValues = append(batch.keyValues, KeyValue{key, value})
  }
  return batch.batch.Put(key, value)
}

func (batch *BatchWrapper) ValueSize() int {
  return batch.batch.ValueSize()
}

func (batch *BatchWrapper) Write() error {
  if batch.writeStream != nil {
    op, err := WriteOperation(batch)
    if err != nil { return err }
    if err = batch.writeStream.Emit(op); err != nil {
      return err
    }
  }
  return batch.batch.Write()
}

func (batch *BatchWrapper) GetKeyValues() []KeyValue {
  return batch.keyValues
}

type DBWrapper struct {
  db ethdb.Database
  writeStream LogProducer
  readStream LogProducer
}

func (db *DBWrapper) Put(key, value []byte) error {
  if db.writeStream != nil {
    op, err := PutOperation(key, value)
    if err != nil { return err }
    if err = db.writeStream.Emit(op); err != nil {
      return err
    }
  }
  return db.db.Put(key, value)
}

func (db *DBWrapper) Get(key []byte) ([]byte, error) {
  if db.readStream != nil {
    op, _ := GetOperation(key)
    db.readStream.Emit(op)
  }
  return db.db.Get(key)
}

func (db *DBWrapper) Has(key []byte) (bool, error) {
  if db.readStream != nil {
    op, _ := HasOperation(key)
    db.readStream.Emit(op)
  }
  return db.db.Has(key)
}

func (db *DBWrapper) Delete(key []byte) error {
  if db.writeStream != nil {
    op, err := DeleteOperation(key)
    if err != nil { return err }
    if err = db.writeStream.Emit(op); err != nil {
      return err
    }
  }
  return db.db.Delete(key)
}

func (db *DBWrapper) Close() {
  db.writeStream.Close()
  db.readStream.Close()
  db.db.Close()
}

func (db *DBWrapper) NewBatch() ethdb.Batch {
  dbBatch := db.db.NewBatch()
  return &BatchWrapper{dbBatch, db.writeStream, []KeyValue{}}
}

func NewDBWrapper(db ethdb.Database, writeStream, readStream LogProducer) ethdb.Database {
  return &DBWrapper{db, writeStream, readStream}
}
