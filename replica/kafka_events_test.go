package replica

import (
  "math/big"
  "github.com/ethereum/go-ethereum/common"
  "github.com/ethereum/go-ethereum/core"
  "github.com/ethereum/go-ethereum/core/types"
  "reflect"
  "runtime"
  "testing"
  // "time"
  // "fmt"
)

func getTestProducer() *KafkaEventProducer {
  return &KafkaEventProducer{
    nil,
    "",
    false,
  }
}

func getTestConsumer() (*KafkaEventConsumer, chan []*types.Log, chan core.RemovedLogsEvent, chan core.ChainEvent, chan core.ChainHeadEvent, chan core.ChainSideEvent, func()) {
  consumer := &KafkaEventConsumer {
    recoverySize: 128,
    consumer: nil,
    oldMap: make(map[common.Hash]*core.ChainEvent),
    currentMap: make(map[common.Hash]*core.ChainEvent),
    ready: make(chan struct{}),
  }
  logsEventCh := make(chan []*types.Log, 100)
  logsEventSub := consumer.SubscribeLogsEvent(logsEventCh)
  removedLogsEventCh := make(chan core.RemovedLogsEvent, 100)
  removedLogsEvenSub := consumer.SubscribeRemovedLogsEvent(removedLogsEventCh)
  chainEventCh := make(chan core.ChainEvent, 100)
  chainEventSub := consumer.SubscribeChainEvent(chainEventCh)
  chainHeadEventCh := make(chan core.ChainHeadEvent, 100)
  chainHeadEvenSub := consumer.SubscribeChainHeadEvent(chainHeadEventCh)
  chainSideEventCh := make(chan core.ChainSideEvent, 100)
  chainSideEvenSub := consumer.SubscribeChainSideEvent(chainSideEventCh)
  close := func() {
    logsEventSub.Unsubscribe()
    removedLogsEvenSub.Unsubscribe()
    chainEventSub.Unsubscribe()
    chainHeadEvenSub.Unsubscribe()
    chainSideEvenSub.Unsubscribe()
  }
  runtime.Gosched()
  return consumer, logsEventCh, removedLogsEventCh, chainEventCh, chainHeadEventCh, chainSideEventCh, close
}

func getTestHeader(blockNo int64, nonce uint64, h *types.Header) *types.Header {
  parentHash := common.Hash{}
  if h != nil {
    parentHash = types.NewBlock(h, []*types.Transaction{}, []*types.Header{}, []*types.Receipt{}).Hash()
  }
  return &types.Header{
  	ParentHash:  parentHash,
  	UncleHash:   common.Hash{},
  	Coinbase:    common.Address{},
  	Root:        common.Hash{},
  	TxHash:      common.Hash{},
  	ReceiptHash: common.Hash{},
  	Bloom:       types.Bloom{},
  	Difficulty:  big.NewInt(0),
  	Number:      big.NewInt(blockNo),
  	GasLimit:    0,
  	GasUsed:     0,
  	Time:        0,
  	Extra:       []byte{},
  	MixDigest:   common.Hash{},
  	Nonce:       types.EncodeNonce(nonce),
  }
}

func getTestLog(block *types.Block) *types.Log {
  return &types.Log {
  	Address: common.Address{},
  	Topics: []common.Hash{},
  	Data: []byte{},
  	BlockNumber: block.Number().Uint64(),
  	TxHash: common.Hash{},
  	TxIndex: 0,
  	BlockHash: block.Hash(),
  	Index: 0,
  }
}

func TestGetProducerMessages(t *testing.T) {
  producer := getTestProducer()
  header := getTestHeader(0, 0, nil)
  block := types.NewBlock(header, []*types.Transaction{}, []*types.Header{}, []*types.Receipt{})
  event := core.ChainEvent{Block: block, Hash: block.Hash(), Logs: []*types.Log{getTestLog(block)}}
  messages, err := producer.getMessages(event)
  if err != nil {
    t.Errorf(err.Error())
  }
  if n := len(messages); n != 3 {
    t.Errorf("Expected 3 messages, got %v", n)
  }
  if messages[0][0] != BlockMsg { t.Errorf("Message 0 should be block Msg, got %v", messages[0][0])}
  if messages[1][0] != LogMsg { t.Errorf("Message 1 should be log Msg, got %v", messages[1][0])}
  if messages[2][0] != EmitMsg { t.Errorf("Message 2 should be emit Msg, got %v", messages[2][0])}
}

func expectToConsume(name string, ch interface{}, count int, t *testing.T) {
  chanval := reflect.ValueOf(ch)

  for i := 0; i < count; i++ {
    chosen, _, _ := reflect.Select([]reflect.SelectCase{
      reflect.SelectCase{Dir: reflect.SelectRecv, Chan: chanval},
      reflect.SelectCase{Dir: reflect.SelectDefault},
    })
    if chosen == 1 {
      t.Errorf("%v: Expected %v items, got %v", name, count, i)
    }
  }
  chosen, _, _ := reflect.Select([]reflect.SelectCase{
    reflect.SelectCase{Dir: reflect.SelectRecv, Chan: chanval},
    reflect.SelectCase{Dir: reflect.SelectDefault},
  })
  if chosen == 0 {
    t.Errorf("%v: Expected %v items, got %v", name, count, count+1)
  }
}

func getMessages(header *types.Header, producer *KafkaEventProducer, t *testing.T) [][]byte {
  block := types.NewBlock(header, []*types.Transaction{}, []*types.Header{}, []*types.Receipt{})
  event := core.ChainEvent{Block: block, Hash: block.Hash(), Logs: []*types.Log{getTestLog(block)}}
  messages, err := producer.getMessages(event)
  if err != nil { t.Errorf(err.Error()) }
  return messages
}

func TestGetConsumerMessages(t *testing.T) {
  producer := getTestProducer()
  messages := getMessages(getTestHeader(0, 0, nil), producer, t)
  consumer, logsEventCh, removedLogsEventCh, chainEventCh, chainHeadEventCh, chainSideEventCh, close := getTestConsumer()
  defer close()
  for _, msg := range messages {
    if err := consumer.processEvent(msg[0], msg[1:]); err != nil { t.Errorf(err.Error()) }
  }
  runtime.Gosched()
  expectToConsume("logsEventCh", logsEventCh, 1, t)
  expectToConsume("removedLogsEventCh", removedLogsEventCh, 0, t)
  expectToConsume("chainEventCh", chainEventCh, 1, t)
  expectToConsume("chainHeadEventCh", chainHeadEventCh, 1, t)
  expectToConsume("chainSideEventCh", chainSideEventCh, 0, t)
}

func TestPreReorgMessages(t *testing.T) {
  producer := getTestProducer()
  root := getTestHeader(0, 0, nil)
  messages := getMessages(root, producer, nil)
  messages = append(messages, getMessages(getTestHeader(1, 0, root), producer, t)...)
  messages = append(messages, getMessages(getTestHeader(1, 1, root), producer, t)...)
  consumer, logsEventCh, removedLogsEventCh, chainEventCh, chainHeadEventCh, chainSideEventCh, close := getTestConsumer()
  defer close()
  for _, msg := range messages {
    if err := consumer.processEvent(msg[0], msg[1:]); err != nil { t.Errorf(err.Error()) }
  }
  runtime.Gosched()
  expectToConsume("logsEventCh", logsEventCh, 2, t)
  expectToConsume("removedLogsEventCh", removedLogsEventCh, 0, t)
  expectToConsume("chainEventCh", chainEventCh, 2, t)
  expectToConsume("chainHeadEventCh", chainHeadEventCh, 2, t)
  expectToConsume("chainSideEventCh", chainSideEventCh, 0, t)
}
func TestNoReorgMessages(t *testing.T) {
  producer := getTestProducer()
  root := getTestHeader(0, 0, nil)
  messages := getMessages(root, producer, nil)
  base := getTestHeader(1, 0, root)
  messages = append(messages, getMessages(base, producer, t)...)
  messages = append(messages, getMessages(getTestHeader(1, 1, root), producer, t)...)
  messages = append(messages, getMessages(getTestHeader(2, 0, base), producer, t)...)
  consumer, logsEventCh, removedLogsEventCh, chainEventCh, chainHeadEventCh, chainSideEventCh, close := getTestConsumer()
  defer close()
  for _, msg := range messages {
    if err := consumer.processEvent(msg[0], msg[1:]); err != nil { t.Errorf(err.Error()) }
  }
  runtime.Gosched()
  expectToConsume("logsEventCh", logsEventCh, 3, t)
  expectToConsume("removedLogsEventCh", removedLogsEventCh, 0, t)
  expectToConsume("chainEventCh", chainEventCh, 3, t)
  expectToConsume("chainHeadEventCh", chainHeadEventCh, 3, t)
  expectToConsume("chainSideEventCh", chainSideEventCh, 0, t)
}
func TestReorgMessages(t *testing.T) {
  producer := getTestProducer()
  root := getTestHeader(0, 0, nil)
  messages := getMessages(root, producer, nil)
  base := getTestHeader(1, 1, root)
  messages = append(messages, getMessages(getTestHeader(1, 2, root), producer, t)...)
  messages = append(messages, getMessages(base, producer, t)...)
  messages = append(messages, getMessages(getTestHeader(2, 3, base), producer, t)...)
  consumer, logsEventCh, removedLogsEventCh, chainEventCh, chainHeadEventCh, chainSideEventCh, close := getTestConsumer()
  defer close()
  for _, msg := range messages {
    if err := consumer.processEvent(msg[0], msg[1:]); err != nil { t.Errorf(err.Error()) }
  }
  runtime.Gosched()
  expectToConsume("logsEventCh", logsEventCh, 4, t)
  expectToConsume("removedLogsEventCh", removedLogsEventCh, 1, t)
  expectToConsume("chainEventCh", chainEventCh, 4, t)
  expectToConsume("chainHeadEventCh", chainHeadEventCh, 4, t)
  expectToConsume("chainSideEventCh", chainSideEventCh, 1, t)
}
