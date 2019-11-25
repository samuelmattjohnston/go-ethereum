package cdc

import (
  "github.com/Shopify/sarama"
  "github.com/ethereum/go-ethereum/log"
  "net/url"
  "strings"
  "time"
)


func ParseKafkaURL(brokerURL string) ([]string, *sarama.Config) {
  parsedURL, _ := url.Parse("kafka://" + brokerURL)
  config := sarama.NewConfig()
  config.Version = sarama.V2_1_0_0
  if parsedURL.Query().Get("tls") == "1" {
    config.Net.TLS.Enable = true
  }
  if parsedURL.User != nil {
    config.Net.SASL.Enable = true
    config.Net.SASL.User = parsedURL.User.Username()
    config.Net.SASL.Password, _ = parsedURL.User.Password()
  }
  return strings.Split(parsedURL.Host, ","), config
}

type KafkaLogProducer struct {
  producer sarama.AsyncProducer
  topic string
  closed bool
}

func (producer *KafkaLogProducer) Close() {
  producer.closed = true
  producer.producer.Close()
}
func (producer *KafkaLogProducer) Start(duration time.Duration) {
  go func() {
    futureTimer := time.NewTicker(duration)
    heartbeatBytes := HeartbeatOperation().Bytes()
    for range futureTimer.C {
      if producer.closed {
        break
      }
      producer.Emit(heartbeatBytes)
    }
  }()
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

func CreateTopicIfDoesNotExist(brokerAddr, topic string) error {
  brokerList, config := ParseKafkaURL(brokerAddr)
  client, err := sarama.NewClient(brokerList, config)
  if err != nil {
    return err
  }
  defer client.Close()
  broker, err := client.Controller()
  if err != nil {
    return err
  }
  log.Info("Getting metadata")
  broker.Open(config)
  defer broker.Close()
  response, err := broker.GetMetadata(&sarama.MetadataRequest{Topics: []string{topic}})
  // log.Info("Got here", "err", err, "topics", *response.Topics[0])
  if err != nil {
    log.Error("Error getting metadata", "err", err)
    return err
  }
  if len(response.Topics) == 0 || len(response.Topics[0].Partitions) == 0 {
    log.Info("Attempting to create topic")
    topicDetails := make(map[string]*sarama.TopicDetail)
    configEntries := make(map[string]*string)
    compressionType := "snappy"
    configEntries["compression.type"] = &compressionType
    replicationFactor := int16(len(client.Brokers()))
    if replicationFactor > 3 {
      // If we have more than 3 brokers, only replicate to 3
      replicationFactor = 3
    }
    topicDetails[topic] = &sarama.TopicDetail{
      ConfigEntries: configEntries,
      NumPartitions: 1,
      ReplicationFactor: replicationFactor,
    }
    r, err := broker.CreateTopics(&sarama.CreateTopicsRequest{
      // Version: 2,
      Timeout: 5 * time.Second,
      TopicDetails: topicDetails,
    })
    if err != nil {
      log.Error("Error creating topic", "error", err, "response", r)
      return err
    }
    if err, _ := r.TopicErrors[topic]; err != nil && err.Err != sarama.ErrNoError {
      log.Error("topic error", "err", err)
      return err
    }
    log.Info("Topic created without errors")
  }
  return nil
}

func NewKafkaLogProducerFromURL(brokerURL, topic string) (LogProducer, error) {
  brokers, config := ParseKafkaURL(brokerURL)
  if err := CreateTopicIfDoesNotExist(brokerURL, topic); err != nil {
    return nil, err
  }
  producer, err := sarama.NewAsyncProducer(brokers, config)
  if err != nil {
    return nil, err
  }
  return NewKafkaLogProducer(producer, topic), nil
}

func NewKafkaLogProducer(producer sarama.AsyncProducer, topic string) (LogProducer) {
  logProducer := &KafkaLogProducer{producer, topic, false}
  // TODO: Make duration configurable?
  logProducer.Start(30 * time.Second)
  return logProducer
}

type KafkaLogConsumer struct {
  consumer sarama.PartitionConsumer
  topic string
  batchHandler *BatchHandler
  ready chan struct{}
  topicExists bool
}

func (consumer *KafkaLogConsumer) Messages() <-chan *Operation {
  if consumer.batchHandler != nil {
    return consumer.batchHandler.outputChannel
  }
  inputChannel := consumer.consumer.Messages()
  consumer.batchHandler = NewBatchHandler()
  if !consumer.topicExists {
    consumer.ready <- struct{}{}
    consumer.ready = nil
  }
  go func() {
    for input := range inputChannel {
      if consumer.ready != nil {
        if consumer.consumer.HighWaterMarkOffset() - input.Offset <= 1 {
          consumer.ready <- struct{}{}
          consumer.ready = nil
        }
      }
      if err := consumer.batchHandler.ProcessInput(input.Value, input.Topic, input.Offset, input.Timestamp); err != nil {
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

func (consumer *KafkaLogConsumer) TopicName() string {
  return consumer.topic
}

func NewKafkaLogConsumer(consumer sarama.Consumer, topic string, offset int64, client sarama.Client) (LogConsumer, error) {
  partitionConsumer, err := consumer.ConsumePartition(topic, 0, offset)
  if err != nil {
    return nil, err
  }
  var highOffset int64
  if client != nil {
    highOffset, _ = client.GetOffset(topic, 0, sarama.OffsetNewest)
  }
  return &KafkaLogConsumer{partitionConsumer, topic, nil, make(chan struct{}), (highOffset > 0)}, nil
}

func NewKafkaLogConsumerFromURL(brokerURL, topic string, offset int64) (LogConsumer, error) {
  brokers, config := ParseKafkaURL(brokerURL)
  if err := CreateTopicIfDoesNotExist(brokerURL, topic); err != nil {
    return nil, err
  }
  config.Version = sarama.V2_1_0_0
  client, err := sarama.NewClient(brokers, config)
  if err != nil {
    return nil, err
  }
  consumer, err := sarama.NewConsumerFromClient(client)
  if err != nil {
    return nil, err
  }
  return NewKafkaLogConsumer(consumer, topic, offset, client)
}
