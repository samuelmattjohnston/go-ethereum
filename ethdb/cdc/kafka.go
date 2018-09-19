package cdc

import (
  "github.com/Shopify/sarama"
  "log"
  "fmt"
)

type KafkaLogProducer struct {
  producer sarama.AsyncProducer
  topic string
}

func (producer *KafkaLogProducer) Close() {
  producer.producer.Close()
}

func (producer *KafkaLogProducer) Emit(op *Operation) error {
  select {
  case producer.producer.Input() <- &sarama.ProducerMessage{Topic: producer.topic, Value: sarama.ByteEncoder(op.Bytes())}:
  case err := <-producer.producer.Errors():
    // TODO: If we get an error here, that indicates a problem with an earlier
    // write.
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
  go func() {
    for input := range inputChannel {
      fmt.Printf("Offset: %v/%v\n", input.Offset, consumer.consumer.HighWaterMarkOffset())
      if consumer.consumer.HighWaterMarkOffset() - input.Offset <= 1 {
        if consumer.ready != nil {
          consumer.ready <- struct{}{}
          consumer.ready = nil
        }
      }
      op, err := OperationFromBytes(input.Value, input.Topic, input.Offset)
      if err != nil {
        log.Printf("Message(topic=%v, partition=%v, offset=%v) is not a valid operation: %v\n", input.Topic, input.Partition, input.Offset, err.Error())
      }
      consumer.outputChannel <- op
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
