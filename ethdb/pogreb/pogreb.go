package pogreb

import (
  "fmt"
  "github.com/akrylysov/pogreb"
  "github.com/ethereum/go-ethereum/ethdb"
)

type Database struct {
  db *pogreb.DB
}

func NewDatabase(path string) (ethdb.KeyValueStore, error) {
  db, err := pogreb.Open(path, nil)
  return &Database{db}, err
}

func (db *Database) Has(key []byte) (bool, error)  {
  return db.db.Has(key)
}
func (db *Database) Get(key []byte) ([]byte, error)  {
  return db.db.Get(key)
}
func (db *Database) Put(key []byte, value []byte) error  {
  return db.db.Put(key, value)
}
func (db *Database) Delete(key []byte) error  {
  return db.db.Delete(key)
}
func (db *Database) Stat(property string) (string, error)  {
  return "unknown", nil
}
func (db *Database) Compact(start []byte, limit []byte) error  {
  return nil
}

func (db *Database) Close() error {
  return db.db.Close()
}

func (db *Database) NewIterator() ethdb.Iterator {
  return &iterator{it: db.db.Items()}
}
func (db *Database) NewIteratorWithStart(start []byte) ethdb.Iterator {
  return &iterator{err: fmt.Errorf("IteratorWithStart unsupported")}
}
func (db *Database) NewIteratorWithPrefix(prefix []byte) ethdb.Iterator {
  return &iterator{err: fmt.Errorf("IteratorWithPrefix unsupported")}
}
func (db *Database) NewBatch() (ethdb.Batch) {
  return &batch{db: db, kv: []kv{}}
}

type iterator struct {
  it *pogreb.ItemIterator
  key []byte
  val []byte
  err error
}

func (it *iterator) Next() bool {
  it.key, it.val, it.err = it.it.Next()
  return it.err == nil
}
func (it *iterator) Error() error {
  return it.err
}
func (it *iterator) Key() []byte {
  return it.key
}
func (it *iterator) Value() []byte {
  return it.val
}
func (it *iterator) Release() { }

type kv struct {
  key []byte
  value []byte
  delete bool
}

type batch struct {
  kv []kv
  size int
  db *Database
}
func (b *batch) Put(key []byte, value []byte) error  {
  b.kv = append(b.kv, kv{key: key, value: value})
  b.size += len(key) + len(value)
  return nil
}
func (b *batch) ValueSize() int {
  return b.size
}
func (b *batch) Delete(key []byte) error  {
  b.kv = append(b.kv, kv{key: key, delete: true})
  return nil
}
func (b *batch) Replay(w ethdb.KeyValueWriter) error {
  for _, pair := range b.kv {
    if pair.delete {
      if err := w.Delete(pair.key); err != nil {
        return err
      }
    } else {
      if err := w.Put(pair.key, pair.value); err != nil {
        return err
      }
    }
  }
  return nil
}

func (b *batch) Write() error {
  return b.Replay(b.db)
}

func (b *batch) Reset() {
  b.kv = []kv{}
}
