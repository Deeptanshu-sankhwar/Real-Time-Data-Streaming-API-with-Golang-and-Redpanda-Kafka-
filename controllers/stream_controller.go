package controllers

import (
	"context"
	"net/http"
	"real_time_data_streaming/kafka"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

type StreamController struct {
	KafkaClient *kafka.KafkaClient
	upgrader    websocket.Upgrader
	logger      *zap.Logger
	streamKeys  map[string]string
}

var MaxMessageCount = 100

func NewStreamController(kc *kafka.KafkaClient, logger *zap.Logger) *StreamController {
	return &StreamController{
		KafkaClient: kc,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin:     func(r *http.Request) bool { return true },
		},
		logger:     logger,
		streamKeys: make(map[string]string),
	}
}

// This function checks if the provided API key has access to the stream_id
func (sc *StreamController) checkStreamAccess(c *gin.Context, streamID string) bool {
	apiKey := c.GetHeader("X-API-Key")
	if sc.streamKeys[streamID] != apiKey {
		sc.logger.Warn("Unauthorized stream access attempt", zap.String("stream_id", streamID), zap.String("api_key", apiKey))
		c.JSON(http.StatusForbidden, gin.H{"error": "Forbidden: Access to this stream is not allowed"})
		return false
	}
	return true
}

// This function initializes a concurrent stream
func (sc *StreamController) StartStream(c *gin.Context) {
	streamID := c.PostForm("stream_id")

	apiKey := c.GetHeader("X-API-Key")

	sc.streamKeys[streamID] = apiKey

	sc.KafkaClient.StartStream(streamID)

	go func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		messages, err := sc.KafkaClient.ConsumeMessages(ctx, streamID, func(msg string) {
			sc.KafkaClient.StreamChannels[streamID] <- msg
		}, MaxMessageCount)

		if err != nil {
			sc.logger.Error("Error consuming messages for stream with", zap.String("stream id", streamID), zap.Error(err))
			return
		}

		for _, msg := range messages {
			sc.logger.Info("Consumed message from stream with ", zap.String("stream id", streamID), zap.String("message", msg))
		}
	}()

	c.JSON(http.StatusOK, gin.H{"message": "Stream started", "stream_id": streamID})
}

// This function sends data to both Kafka and the channel
func (sc *StreamController) SendStreamData(c *gin.Context) {
	streamID := c.Param("stream_id")
	if !sc.checkStreamAccess(c, streamID) {
		return
	}

	message := c.PostForm("message")

	if err := sc.KafkaClient.ProduceMessage(streamID, message); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to send message to Kafka"})
		return
	}

	if ch, ok := sc.KafkaClient.StreamChannels[streamID]; ok {
		ch <- message
	}

	c.JSON(http.StatusOK, gin.H{"message": "Data sent to stream " + streamID})
}

// This function reads a limited number of messages (MaxMessageCount) from the stream's channel and returns them in the HTTP response
func (sc *StreamController) GetStreamResults(c *gin.Context) {
	streamID := c.Param("stream_id")
	if !sc.checkStreamAccess(c, streamID) {
		return
	}

	ch, ok := sc.KafkaClient.StreamChannels[streamID]
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "Stream not found"})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	messages := []string{}

	for i := 0; i < MaxMessageCount; i++ {
		select {
		case msg, ok := <-ch:
			if !ok {
				break
			}
			messages = append(messages, msg)
		case <-ctx.Done():
			c.JSON(http.StatusOK, gin.H{"messages": messages, "info": "Partial or limited result due to timeout"})
			return
		}
	}

	c.JSON(http.StatusOK, gin.H{"messages": messages})
}

// This function adds a prefix of time stamp to each message to perform or aid real-time processing
func (sc *StreamController) ProcessMessage(msg string) string {
	return time.Now().Format(time.RFC3339) + " - " + msg
}

// This function sets up a WebSocket to stream processed data back to the client
func (sc *StreamController) StreamResults(c *gin.Context) {
	streamID := c.Param("stream_id")
	if !sc.checkStreamAccess(c, streamID) {
		return
	}

	conn, err := sc.upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		sc.logger.Error("Failed to upgrade WebSocket connection: ", zap.Error(err))
		return
	}
	defer conn.Close()

	ch, ok := sc.KafkaClient.StreamChannels[streamID]
	if !ok {
		conn.WriteMessage(websocket.TextMessage, []byte("Stream not found"))
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		for {
			select {
			case msg, ok := <-ch:
				if !ok {
					conn.WriteMessage(websocket.TextMessage, []byte("Stream closed"))
					return
				}

				processedMsg := sc.ProcessMessage(msg)

				if err := conn.WriteMessage(websocket.TextMessage, []byte(processedMsg)); err != nil {
					sc.logger.Error("Failed to send message to WebSocket: ", zap.Error(err))
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			sc.logger.Error("Client disconnected:", zap.Error(err))
			break
		}
	}
}
