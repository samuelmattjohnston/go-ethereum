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
)

type Replica struct {
  db ethdb.Database
  hc *core.HeaderChain
  chainConfig *params.ChainConfig
  bc *core.BlockChain
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

func NewReplica(db ethdb.Database, config *eth.Config, ctx *node.ServiceContext, kafkaSourceBroker []string, kafkaTopic string) (*Replica, error) {
  chainConfig, _, _ := core.SetupGenesisBlock(db, config.Genesis)
  engine := eth.CreateConsensusEngine(ctx, &config.Ethash, chainConfig, db)
  hc, err := core.NewHeaderChain(db, chainConfig, engine, func() bool { return false })
  if err != nil {
    return nil, err
  }
  bc, err := core.NewBlockChain(db, &core.CacheConfig{Disabled: true}, chainConfig, engine, vm.Config{})
  if err != nil {
    return nil, err
  }
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
  go func() {
    for operation := range consumer.Messages() {
      operation.Apply(db)
    }
  }()
  return &Replica{db, hc, chainConfig, bc}, nil
}
