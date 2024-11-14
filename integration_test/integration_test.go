package integration_test

import (
	"net/http"
	"net/http/httptest"
	"os"
	"real_time_data_streaming/controllers"
	"real_time_data_streaming/kafka"
	"real_time_data_streaming/middleware"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestStreamLifecycle_Integration(t *testing.T) {
	// Ensure that the Kafka broker is running at localhost:19092
	if err := os.Setenv("API_KEY", "valid_api_key"); err != nil {
		t.Fatal("Failed to set API_KEY environment variable")
	}
	logger, _ := zap.NewDevelopment()

	kafkaClient, err := kafka.NewKafkaClient([]string{"localhost:19092"}, logger)
	assert.NoError(t, err)

	streamController := controllers.NewStreamController(kafkaClient, logger)

	router := gin.Default()
	router.Use(middleware.APIKeyAuthMiddleware(logger))

	router.POST("/stream/start", streamController.StartStream)
	router.POST("/stream/:stream_id/send", streamController.SendStreamData)
	router.GET("/stream/:stream_id/results", streamController.GetStreamResults)

	req, _ := http.NewRequest("POST", "/stream/start", nil)
	req.Header.Set("X-API-Key", "blockhouse")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	streamID := "test_stream"

	req, _ = http.NewRequest("POST", "/stream/"+streamID+"/send", nil)
	req.Header.Set("X-API-Key", "blockhouse")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	req, _ = http.NewRequest("GET", "/stream/"+streamID+"/results", nil)
	req.Header.Set("X-API-Key", "blockhouse")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "Hello, Kafka!")
}
