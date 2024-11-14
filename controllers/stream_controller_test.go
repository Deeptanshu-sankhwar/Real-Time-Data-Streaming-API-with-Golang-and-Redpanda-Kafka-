package controllers_test

import (
	"net/http"
	"net/http/httptest"
	"real_time_data_streaming/controllers"
	"real_time_data_streaming/kafka"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestStartStream(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	kafkaClient := &kafka.KafkaClient{
		StreamChannels: make(map[string]chan string),
	}
	streamController := controllers.NewStreamController(kafkaClient, logger)

	router := gin.Default()
	router.POST("/stream/start", streamController.StartStream)

	req, _ := http.NewRequest("POST", "/stream/start", nil)
	req.Header.Set("X-API-Key", "blockhouse")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, kafkaClient.StreamChannels, "test_stream")
}

func TestSendStreamData(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	kafkaClient := &kafka.KafkaClient{
		StreamChannels: make(map[string]chan string),
	}
	streamController := controllers.NewStreamController(kafkaClient, logger)

	router := gin.Default()
	router.POST("/stream/:stream_id/send", streamController.SendStreamData)

	kafkaClient.StreamChannels["test_stream"] = make(chan string, 1)
	req, _ := http.NewRequest("POST", "/stream/test_stream/send", nil)
	req.Header.Set("X-API-Key", "blockhouse")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}
