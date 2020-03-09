package replica

import (
  "github.com/Shopify/sarama"
  // "log"
  "fmt"
  "github.com/ethereum/go-ethereum/common"
  "github.com/ethereum/go-ethereum/core"
  "github.com/ethereum/go-ethereum/core/types"
  "github.com/ethereum/go-ethereum/event"
  "github.com/ethereum/go-ethereum/rlp"
  "github.com/ethereum/go-ethereum/log"
  "github.com/ethereum/go-ethereum/ethdb/cdc"
  // "encoding/hex"
)

type KafkaEventProducer struct {
  producer sarama.SyncProducer
  topic string
  closed bool
}

func (producer *KafkaEventProducer) Close() {
  producer.closed = true
  producer.producer.Close()
}

func (producer *KafkaEventProducer) Emit(chainEvent core.ChainEvent) error {
  blockBytes, err := rlp.EncodeToBytes(chainEvent.Block)
  if err != nil { return err }
  msg := &sarama.ProducerMessage{Topic: producer.topic, Value: sarama.ByteEncoder(append([]byte{0}, blockBytes...))}
  if _, _, err = producer.producer.SendMessage(msg); err != nil { return err }
  for _, log := range chainEvent.Logs {
    logBytes, err := rlp.EncodeToBytes(log)
    msg :=  &sarama.ProducerMessage{Topic: producer.topic, Value: sarama.ByteEncoder(append([]byte{1}, logBytes...))}
    if _, _, err = producer.producer.SendMessage(msg); err != nil { return err }
  }
  msg = &sarama.ProducerMessage{Topic: producer.topic, Value: sarama.ByteEncoder(append([]byte{2}, chainEvent.Hash[:]...))}
  if _, _, err = producer.producer.SendMessage(msg); err != nil { return err }
  return nil
}

type ChainEventSubscriber interface {
  SubscribeChainEvent(chan<- core.ChainEvent) event.Subscription
}

func (producer *KafkaEventProducer) RelayEvents(bc ChainEventSubscriber) {
  ceCh := make(chan core.ChainEvent, 100)
  subscription := bc.SubscribeChainEvent(ceCh)
  go func() {
    for ce := range ceCh {
      producer.Emit(ce)
    }
    log.Warn("Event emitter shutting down")
    subscription.Unsubscribe()
  }()
}

func NewKafkaEventProducerFromURLs(brokerURL, topic string) (EventProducer, error) {
  configEntries := make(map[string]*string)
  configEntries["retention.ms"] = strPtr("3600000")
  brokers, config := cdc.ParseKafkaURL(brokerURL)
  if err := cdc.CreateTopicIfDoesNotExist(brokerURL, topic, 1, configEntries); err != nil {
    return nil, err
  }
  config.Producer.Return.Successes=true
  producer, err := sarama.NewSyncProducer(brokers, config)
  if err != nil {
    return nil, err
  }
  return NewKafkaEventProducer(producer, topic), nil
}

func NewKafkaEventProducer(producer sarama.SyncProducer, topic string) (EventProducer) {
  return &KafkaEventProducer{producer, topic, false}
}

type KafkaEventConsumer struct {
  recoverySize int
  logsFeed event.Feed
  removedLogsFeed event.Feed
  chainFeed event.Feed
  chainHeadFeed event.Feed
  chainSideFeed event.Feed
  consumer sarama.PartitionConsumer
  topic string
  ready chan struct{}
  lastEmittedBlock common.Hash
}

func (consumer KafkaEventConsumer) SubscribeLogsEvent(ch chan<- []*types.Log) event.Subscription {
  return consumer.logsFeed.Subscribe(ch)
}
func (consumer KafkaEventConsumer) SubscribeRemovedLogsEvent(ch chan<- core.RemovedLogsEvent) event.Subscription {
  return consumer.removedLogsFeed.Subscribe(ch)
}
func (consumer KafkaEventConsumer) SubscribeChainEvent(ch chan<- core.ChainEvent) event.Subscription {
  return consumer.chainFeed.Subscribe(ch)
}
func (consumer KafkaEventConsumer) SubscribeChainHeadEvent(ch chan<- core.ChainHeadEvent) event.Subscription {
  return consumer.chainHeadFeed.Subscribe(ch)
}
func (consumer KafkaEventConsumer) SubscribeChainSideEvent(ch chan<- core.ChainSideEvent) event.Subscription {
  return consumer.chainSideFeed.Subscribe(ch)
}

func (consumer KafkaEventConsumer) Start() {
  inputChannel := consumer.consumer.Messages()
  go func() {
    oldMap := make(map[common.Hash]*core.ChainEvent)
    currentMap := make(map[common.Hash]*core.ChainEvent)
    MAIN:
    for input := range inputChannel {
      if consumer.ready != nil {
        if consumer.consumer.HighWaterMarkOffset() - input.Offset <= 1 {
          consumer.ready <- struct{}{}
          consumer.ready = nil
        }
      }
      msgType := input.Value[0]
      msg := input.Value[1:]
      if msgType == 0 {
        // Message contains the block. Set up a ChainEvent for this block
        block := &types.Block{}
        if err := rlp.DecodeBytes(msg, block); err != nil {
          log.Error("Error decoding block", "offset", input.Offset, "err", err, "msg", input.Value)
          continue
        }
        hash := block.Hash()
        if _, ok := currentMap[hash]; !ok {
          // First time we've seen the block.
          currentMap[hash] = &core.ChainEvent{Block: block, Hash: hash, Logs: []*types.Log{}}
        }
      } else if msgType == 1{
        // Message contains a log. Add it to the chain event for the block.
        logRecord := &types.Log{}
        if err := rlp.DecodeBytes(msg, logRecord); err != nil {
          log.Error("Error decoding log", "offset", input.Offset, "err", err, "msg", input.Value)
          continue
        }
        if _, ok := currentMap[logRecord.BlockHash]; !ok {
          if ce, ok := oldMap[logRecord.BlockHash]; ok {
            currentMap[logRecord.BlockHash] = ce
          } else {
            log.Error("Received log for unknown block", "offset", input.Offset, "msg", input.Value, "blockHash", logRecord.BlockHash)
            continue
          }
        }
        for _, l := range currentMap[logRecord.BlockHash].Logs {
          // TODO: Consider some separate map for faster lookups.  Not an
          // immediate concern, as block log counts are in the low hundreds,
          // and this implementation should be O(n*log(n))
          if l.Index == logRecord.Index {
            // Log is already in the list, don't add it again
            continue MAIN
          }
        }
        currentMap[logRecord.BlockHash].Logs = append(currentMap[logRecord.BlockHash].Logs, logRecord)
      } else if msgType == 2 {
        // Last message of block. Emit the chain event on appropriate feeds.
        hash := common.BytesToHash(input.Value[1:])
        event, ok := currentMap[hash]
        if !ok {
          event, ok = oldMap[hash]
          if !ok {
            log.Error("Received emit for unknown block", "hash", hash)
            continue
          }
        }
        emptyHash := common.Hash{}
        if event.Block.Hash() == consumer.lastEmittedBlock {
          // Given multiple masters, we'll see blocks repate
          continue
        }
        if event.Block.ParentHash() == consumer.lastEmittedBlock || consumer.lastEmittedBlock == emptyHash {
          // This is the next logical block or we're just booting up, just emit everything.
          consumer.Emit([]core.ChainEvent{*event}, []core.ChainEvent{})
        } else {
          newBlocks, revertBlocks, err := findCommonAncestor(event, currentMap[consumer.lastEmittedBlock], []map[common.Hash]*core.ChainEvent{currentMap, oldMap})
          if err != nil {
            log.Error("Error finding common ancestor", "newBlock", event.Block.Hash(), "oldBlock", consumer.lastEmittedBlock, "error", err)
            continue
          }
          if len(newBlocks) > 0 {
            // If we have only revert blocks, this is just an out-of-order
            // block, and should be ignored.
            consumer.Emit(newBlocks, revertBlocks)
          }
          if len(currentMap) > consumer.recoverySize {
            oldMap = currentMap
            currentMap = make(map[common.Hash]*core.ChainEvent)
            currentMap[consumer.lastEmittedBlock] = oldMap[consumer.lastEmittedBlock]
          }
        }
      }
    }
  }()
}

func getFromMappings(key common.Hash, mappings []map[common.Hash]*core.ChainEvent) *core.ChainEvent {
  for _, mapping := range mappings {
    if val, ok := mapping[key]; ok {
      return val
    }
  }
  return nil
}

func findCommonAncestor(newHead, oldHead *core.ChainEvent, mappings []map[common.Hash]*core.ChainEvent) ([]core.ChainEvent, []core.ChainEvent, error) {
  reverted := []core.ChainEvent{}
  newBlocks := []core.ChainEvent{*newHead}
  if oldHead == nil {
    return reverted, newBlocks, nil
  }
  for {
    for newHead.Block.NumberU64() > oldHead.Block.NumberU64() + 1 {
      parentHash := newHead.Block.ParentHash()
      newHead = getFromMappings(parentHash, mappings)
      if newHead == nil {
        return reverted, newBlocks, fmt.Errorf("Block %#x missing from database", parentHash)
      }
      newBlocks = append([]core.ChainEvent{*newHead}, newBlocks...)
    }
    if(oldHead.Block.Hash() == newHead.Block.ParentHash())  {
      return reverted, newBlocks, nil
    }
    reverted = append([]core.ChainEvent{*oldHead}, reverted...)
    oldHead = getFromMappings(oldHead.Block.ParentHash(), mappings)
    if oldHead == nil {
      return reverted, newBlocks, fmt.Errorf("Reached genesis without finding common ancestor")
    }
  }
}

func (consumer KafkaEventConsumer) Emit(add []core.ChainEvent, remove []core.ChainEvent) {
  for _, revert := range remove {
    if len(revert.Logs) > 0 {
      consumer.removedLogsFeed.Send(core.RemovedLogsEvent{revert.Logs})
    }
    consumer.chainSideFeed.Send(core.ChainSideEvent{Block: revert.Block})
  }
  for _, newEvent := range add {
    if len(newEvent.Logs) > 0 {
      consumer.logsFeed.Send(newEvent.Logs)
    }
    consumer.chainHeadFeed.Send(newEvent)
    consumer.lastEmittedBlock = newEvent.Hash
  }
}
