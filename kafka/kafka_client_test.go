package kafka_test

import (
	"real_time_data_streaming/kafka"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestProduceMessage_Success(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	// Assuming a mock Kafka producer here with a real Sarama producer
	client, err := kafka.NewKafkaClient([]string{"localhost:19092"}, logger)
	assert.NoError(t, err)

	err = client.ProduceMessage("test_topic", "Hello, Kafka!")
	assert.NoError(t, err)
}

func TestProduceMessage_Failure(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	// Assuming no brokers are running to simulate failure (no broker running at the provided address)
	client, err := kafka.NewKafkaClient([]string{"invalid:9092"}, logger)
	assert.NoError(t, err)

	err = client.ProduceMessage("test_topic", "Hello, Kafka!")
	assert.Error(t, err)
}
