package main

import (
	"log"
	"real_time_data_streaming/controllers"
	"real_time_data_streaming/kafka"
	"real_time_data_streaming/middleware"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"go.uber.org/zap"
)

var logger *zap.Logger

func init() {
	var err error
	logger, err = zap.NewProduction()
	if err != nil {
		log.Fatalf("Failed to initialize Zap logger: %v", err)
	}
	defer logger.Sync()
}

func main() {
	// Load environment variables
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	router := gin.Default()
	router.Use(middleware.APIKeyAuthMiddleware(logger))

	kafkaClient, err := kafka.NewKafkaClient([]string{"localhost:19092"}, logger)
	if err != nil {
		logger.Fatal("Failed to initialize Kafka client", zap.Error(err))
	}

	streamController := controllers.NewStreamController(kafkaClient, logger)

	router.POST("/stream/start", streamController.StartStream)
	router.POST("/stream/:stream_id/send", streamController.SendStreamData)
	router.GET("/stream/:stream_id/results", streamController.GetStreamResults)

	// Webocket endpoint
	router.GET("/stream/:stream_id/stream", streamController.StreamResults)

	logger.Info("Server is starting", zap.String("port", "8082"))
	if err := router.Run(":8082"); err != nil {
		logger.Fatal("Could not start server", zap.Error(err))
	}
}
