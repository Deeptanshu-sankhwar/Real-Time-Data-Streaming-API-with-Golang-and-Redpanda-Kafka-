package middleware

import (
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

func APIKeyAuthMiddleware(logger *zap.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		apiKey := c.GetHeader("X-API-Key")
		requiredAPIKey := os.Getenv("API_KEY")
		// requiredAPIKey := "blockhouse"

		if apiKey == "" || apiKey != requiredAPIKey {
			logger.Warn("Unauthorized access attempt", zap.String("client_api_key", apiKey))
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Unauthorized"})
			return
		}

		c.Next()
	}
}
