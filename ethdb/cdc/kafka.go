package cdc

import (
  "github.com/ethereum/go-ethereum/rlp"
  "github.com/Shopify/sarama"
  "log"
)

type KafkaLogProducer struct {
  producer sarama.AsyncProducer
  topic string
}

func (producer *KafkaLogProducer) Close() {
  producer.producer.Close()
}

func (producer *KafkaLogProducer) Emit(data []byte) error {
  select {
  case producer.producer.Input() <- &sarama.ProducerMessage{Topic: producer.topic, Value: sarama.ByteEncoder(data)}:
  case err := <-producer.producer.Errors():
    // TODO: If we get an error here, that indicates a problem with an earlier
    // write.
    log.Printf("Error emitting: %v", err.Error())
    return err
  }
  return nil
}

func NewKafkaLogProducerFromURLs(brokers []string, topic string) (LogProducer, error) {
  config := sarama.NewConfig()
  producer, err := sarama.NewAsyncProducer(brokers, config)
  if err != nil {
    return nil, err
  }
  return NewKafkaLogProducer(producer, topic), nil
}

func NewKafkaLogProducer(producer sarama.AsyncProducer, topic string) (LogProducer) {
  return &KafkaLogProducer{producer, topic}
}

type KafkaLogConsumer struct {
  consumer sarama.PartitionConsumer
  topic string
  outputChannel chan *Operation
  ready chan struct{}
}

func (consumer *KafkaLogConsumer) Messages() <-chan *Operation {
  if consumer.outputChannel != nil {
    return consumer.outputChannel
  }
  inputChannel := consumer.consumer.Messages()
  consumer.outputChannel = make(chan *Operation, cap(inputChannel))
  batches := make(map[string][]BatchOperation)
  go func() {
    for input := range inputChannel {
      if consumer.ready != nil {
        if consumer.consumer.HighWaterMarkOffset() - input.Offset <= 1 {
          consumer.ready <- struct{}{}
          consumer.ready = nil
        }
      }
      if input.Value[0] == 255 {
        batchValue := make([]byte, len(input.Value))
        copy(batchValue[:], input.Value[:])
        bop, err := BatchOperationFromBytes(batchValue, input.Topic, input.Offset)
        if err != nil {
          log.Printf("Message(topic=%v, partition=%v, offset=%v) is not a valid operation: %v\n", input.Topic, input.Partition, input.Offset, err.Error())
        }
        batch, ok := batches[string(bop.Batch[:])]
        if !ok {
          batch = []BatchOperation{}
        }
        batches[string(bop.Batch[:])] = append(batch, bop)
      } else {
        op, err := OperationFromBytes(input.Value, input.Topic, input.Offset)
        if op.Op == OpWrite {
          if batch, ok := batches[string(op.Data)]; ok {
            data, err := rlp.EncodeToBytes(batch)
            if err != nil {
              log.Printf("Failed to encode batch operation: %v", err)
            }
            delete(batches, string(op.Data))
            op.Data = append(op.Data, data...)
          } else {
            log.Printf("Could not find matching batch: %#x, (%v known)", op.Data, len(batches))
            continue
          }
        }
        if err != nil {
          log.Printf("Message(topic=%v, partition=%v, offset=%v) is not a valid operation: %v\n", input.Topic, input.Partition, input.Offset, err.Error())
        }
        consumer.outputChannel <- op
      }
    }
  }()
  return consumer.outputChannel
}

func (consumer *KafkaLogConsumer) Ready() <-chan struct{} {
  return consumer.ready
}

func (consumer *KafkaLogConsumer) Close() {
  consumer.consumer.Close()
}

func NewKafkaLogConsumer(consumer sarama.Consumer, topic string, offset int64) (LogConsumer, error) {
  partitionConsumer, err := consumer.ConsumePartition(topic, 0, offset)
  if err != nil {
    return nil, err
  }
  return &KafkaLogConsumer{partitionConsumer, topic, nil, make(chan struct{})}, nil
}

func NewKafkaLogConsumerFromURLs(brokers []string, topic string, offset int64) (LogConsumer, error) {
  config := sarama.NewConfig()
  consumer, err := sarama.NewConsumer(brokers, config)
  if err != nil {
    return nil, err
  }
  return NewKafkaLogConsumer(consumer, topic, offset)
}
