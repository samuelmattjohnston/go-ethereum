package cdc

import (
  "github.com/Shopify/sarama"
  "github.com/ethereum/go-ethereum/log"
)

type KafkaLogProducer struct {
  producer sarama.AsyncProducer
  topic string
}

func (producer *KafkaLogProducer) Close() {
  producer.producer.Close()
}

func (producer *KafkaLogProducer) Emit(data []byte) error {
  log.Debug("Emitting data", "topic", producer.topic, "bytes", len(data))
  select {
  case producer.producer.Input() <- &sarama.ProducerMessage{Topic: producer.topic, Value: sarama.ByteEncoder(data)}:
  case err := <-producer.producer.Errors():
    // TODO: If we get an error here, that indicates a problem with an earlier
    // write.
    log.Error("Error emitting: %v", "err", err.Error())
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
  batchHandler *BatchHandler
  ready chan struct{}
}

func (consumer *KafkaLogConsumer) Messages() <-chan *Operation {
  if consumer.batchHandler != nil {
    return consumer.batchHandler.outputChannel
  }
  inputChannel := consumer.consumer.Messages()
  consumer.batchHandler = NewBatchHandler()
  go func() {
    for input := range inputChannel {
      if consumer.ready != nil {
        if consumer.consumer.HighWaterMarkOffset() - input.Offset <= 1 {
          consumer.ready <- struct{}{}
          consumer.ready = nil
        }
      }
      if err := consumer.batchHandler.ProcessInput(input.Value, input.Topic, input.Offset); err != nil {
        log.Error(err.Error())
      }
    }
  }()
  return consumer.batchHandler.outputChannel
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
