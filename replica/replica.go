package replica

import (
  "errors"
  "encoding/binary"
  "github.com/ethereum/go-ethereum/p2p"
  "github.com/ethereum/go-ethereum/rpc"
  // "github.com/ethereum/go-ethereum/node"
  "github.com/ethereum/go-ethereum/eth"
  "github.com/ethereum/go-ethereum/ethdb"
  "github.com/ethereum/go-ethereum/ethdb/cdc"
  "github.com/ethereum/go-ethereum/node"
  "github.com/ethereum/go-ethereum/core"
  "github.com/ethereum/go-ethereum/core/vm"
  "github.com/ethereum/go-ethereum/internal/ethapi"
  "github.com/ethereum/go-ethereum/params"
  "github.com/Shopify/sarama"
  "fmt"
  "time"
)

type Replica struct {
  db ethdb.Database
  hc *core.HeaderChain
  chainConfig *params.ChainConfig
  bc *core.BlockChain
  transactionProducer TransactionProducer
}

func (r *Replica) Protocols() []p2p.Protocol {
  return []p2p.Protocol{}
}
func (r *Replica) APIs() []rpc.API {
  return append(ethapi.GetAPIs(&ReplicaBackend{
    db: r.db,
    hc: r.hc,
    chainConfig: r.chainConfig,
    bc: r.bc,
    transactionProducer: r.transactionProducer,
  }),
  rpc.API{
    Namespace: "net",
    Version:   "1.0",
    Service:   NewReplicaNetAPI(),
    Public:    true,
  })
}
func (r *Replica) Start(server *p2p.Server) error {
  fmt.Println("Replica.start()")
  return nil
}
func (r *Replica) Stop() error {
  fmt.Println("Replica.stop()")
  return nil
}

// TODO ADD THE CONFIGURATION HERE FOR TRIGGERING POSTBACK
func NewReplica(db ethdb.Database, config *eth.Config, ctx *node.ServiceContext, kafkaSourceBroker []string, kafkaTopic, transactionTopic string) (*Replica, error) {
  offsetBytes, err := db.Get([]byte(fmt.Sprintf("cdc-log-%v-offset", kafkaTopic)))
  var offset int64
  var bytesRead int
  if err != nil || len(offsetBytes) == 0 {
    offset = sarama.OffsetOldest
  } else {
    offset, bytesRead = binary.Varint(offsetBytes)
    if bytesRead <= 0 { return nil, errors.New("Offset buffer too small") }
  }
  consumer, err := cdc.NewKafkaLogConsumerFromURLs(
    kafkaSourceBroker,
    kafkaTopic,
    offset,
  )
  if err != nil { return nil, err }
  fmt.Printf("Pre: %v\n", time.Now())
  // TODO : CREATE THE PRODUCER HERE with if conditionals
  transactionProducer, err := NewKafkaTransactionProducerFromURLs(
    kafkaSourceBroker,
    transactionTopic,
  )
  if err != nil {
    return nil, err
  }
  go func() {
    for operation := range consumer.Messages() {
      operation.Apply(db)
    }
  }()
  <-consumer.Ready()
  fmt.Printf("Post: %v\n", time.Now())
  chainConfig, _, _ := core.SetupGenesisBlock(db, config.Genesis)
  engine := eth.CreateConsensusEngine(ctx, chainConfig, &config.Ethash, []string{}, true, db)
  hc, err := core.NewHeaderChain(db, chainConfig, engine, func() bool { return false })
  if err != nil {
    return nil, err
  }
  bc, err := core.NewBlockChain(db, &core.CacheConfig{Disabled: true}, chainConfig, engine, vm.Config{}, nil)
  if err != nil {
    return nil, err
  }
  return &Replica{db, hc, chainConfig, bc, transactionProducer}, nil
}
