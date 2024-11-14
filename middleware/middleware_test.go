package middleware_test

import (
	"net/http"
	"net/http/httptest"
	"os"
	"real_time_data_streaming/middleware"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestAPIKeyAuthMiddleware_ValidKey(t *testing.T) {
	os.Setenv("API_KEY", "blockhouse")
	logger, _ := zap.NewDevelopment()

	router := gin.Default()
	router.Use(middleware.APIKeyAuthMiddleware(logger))

	router.GET("/protected", func(c *gin.Context) {
		c.String(http.StatusOK, "Welcome")
	})

	req, _ := http.NewRequest("GET", "/protected", nil)
	req.Header.Set("X-API-Key", "blockhouse")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "Welcome", w.Body.String())
}
