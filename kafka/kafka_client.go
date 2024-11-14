package kafka

import (
	"context"
	"os"
	"os/signal"
	"time"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

type Producer interface {
	SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error)
}

type KafkaClient struct {
	consumer       sarama.ConsumerGroup
	StreamChannels map[string]chan string
	logger         *zap.Logger
	producer       Producer
}

// This function initializes a new Kafka client with a producer and consumer
func NewKafkaClient(brokers []string, logger *zap.Logger) (*KafkaClient, error) {
	producerConfig := sarama.NewConfig()
	producerConfig.Producer.Return.Successes = true
	producerConfig.Producer.Retry.Max = 5
	producerConfig.Net.DialTimeout = 10 * time.Second

	producer, err := sarama.NewSyncProducer(brokers, producerConfig)
	if err != nil {
		return nil, err
	}
	logger.Info("Kafka producer created successfully")

	consumerConfig := sarama.NewConfig()
	consumerConfig.Version = sarama.V0_11_0_0
	consumerConfig.Consumer.Retry.Backoff = 2 * time.Second
	consumerConfig.Metadata.RefreshFrequency = 10 * time.Minute
	consumerConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	consumerConfig.Consumer.Offsets.AutoCommit.Enable = true
	consumerConfig.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second

	consumer, err := sarama.NewConsumerGroup(brokers, "streaming-consumer-group", consumerConfig)
	if err != nil {
		return nil, err
	}
	logger.Info("Kafka consumer group created successfully")

	return &KafkaClient{
		producer:       producer,
		consumer:       consumer,
		StreamChannels: make(map[string]chan string),
		logger:         logger,
	}, nil
}

// This function initializes a channel for a new stream
func (kc *KafkaClient) StartStream(streamID string) {
	kc.StreamChannels[streamID] = make(chan string)
	kc.logger.Info("Stream channel initialized", zap.String("stream_id", streamID))
}

// This function closes the channel for a specific stream
func (kc *KafkaClient) StopStream(streamID string) {
	if ch, ok := kc.StreamChannels[streamID]; ok {
		close(ch)
		delete(kc.StreamChannels, streamID)
		kc.logger.Info("Stream channel closed", zap.String("stream_id", streamID))
	}
}

// This function sends a message to the specified topic (stream_id)
func (kc *KafkaClient) ProduceMessage(streamID, message string) error {
	msg := &sarama.ProducerMessage{
		Topic: streamID,
		Value: sarama.StringEncoder(message),
	}
	_, _, err := kc.producer.SendMessage(msg)
	if err != nil {
		kc.logger.Error("Failed to produce message", zap.String("stream_id", streamID), zap.Error(err))
		return err
	}
	kc.logger.Info("Message produced successfully", zap.String("stream_id", streamID), zap.String("message", message))

	return err
}

type consumerGroupHandler struct {
	streamID     string
	handler      func(string)
	maxMessages  int
	messageCount int // To track consumed messages
	messageChan  chan string
	logger       *zap.Logger
}

func (h *consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (h *consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	h.logger.Info("Consuming from topic", zap.String("topic", claim.Topic()), zap.Int32("partition", claim.Partition()))

	for msg := range claim.Messages() {
		h.logger.Info("Received message", zap.String("message", string(msg.Value)))
		h.handler(string(msg.Value))

		sess.MarkMessage(msg, "")

		h.messageCount++
		if h.messageCount >= h.maxMessages {
			h.logger.Info("Reached max messages", zap.Int("maxMessages", h.maxMessages))
			return nil
		}
	}
	return nil
}

// This function consumes messages for a given topic and passes them to the handler.
func (kc *KafkaClient) ConsumeMessages(ctx context.Context, streamID string, handler func(string), maxMessages int) ([]string, error) {
	kc.logger.Info("Starting message consumption", zap.String("stream_id", streamID), zap.Int("maxMessages", maxMessages))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		sigterm := make(chan os.Signal, 1)
		signal.Notify(sigterm, os.Interrupt)
		<-sigterm
		cancel()
	}()

	messageChan := make(chan string, maxMessages)
	defer close(messageChan)

	consumerHandler := &consumerGroupHandler{
		streamID:    streamID,
		handler:     handler,
		maxMessages: maxMessages,
		messageChan: messageChan,
		logger:      kc.logger,
	}

	if err := kc.consumer.Consume(ctx, []string{streamID}, consumerHandler); err != nil {
		kc.logger.Error("Error in consumer group", zap.String("stream_id", streamID), zap.Error(err))
		return nil, err
	}

	messages := make([]string, 0, maxMessages)
	for i := 0; i < maxMessages; i++ {
		select {
		case msg := <-messageChan:
			messages = append(messages, msg)
		case <-ctx.Done():
			kc.logger.Warn("Context timed out while consuming messages", zap.String("stream_id", streamID), zap.Error(ctx.Err()))
			return messages, ctx.Err()
		}
	}

	kc.logger.Info("Completed message consumption", zap.String("stream_id", streamID), zap.Int("message_count", len(messages)))
	return messages, nil
}
