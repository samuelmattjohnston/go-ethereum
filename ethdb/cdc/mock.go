package cdc

type MockLogProducer struct {
  channel chan *Operation
}

func (producer *MockLogProducer) Emit(op *Operation) error {
  producer.channel <- op
  return nil
}

func (producer *MockLogProducer) Close() {}

type MockLogConsumer struct {
  channel <-chan *Operation
}

func (consumer *MockLogConsumer) Messages() (<-chan *Operation) {
  return consumer.channel
}

func (consumer *MockLogConsumer) Close() {}

func MockLogPair() (LogProducer, LogConsumer) {
  channel := make(chan *Operation, 2)
  return &MockLogProducer{channel}, &MockLogConsumer{channel}
}
