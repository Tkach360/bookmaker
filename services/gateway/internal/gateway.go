package internal

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"bookmaker/bets"

	"github.com/gin-gonic/gin"
	"github.com/shopspring/decimal"
)

type Gateway struct {
	BetClient *bets.BetClient
	Router    *gin.Engine
}

func NewGateway(router *gin.Engine, betClient *bets.BetClient) *Gateway {
	return &Gateway{
		BetClient: betClient,
		Router:    router,
	}
}

func (g *Gateway) SetupRouter() {
	// TODO: когда будет больше роутов нужно будет сформировать группы.
	g.Router.LoadHTMLGlob("/app/templates/*.html")
	g.Router.GET("/", g.GetIndex)
	g.Router.GET("/bet/:id", g.GetBet)
	g.Router.POST("/bet", g.CreateBet)
	g.Router.GET("/bet/:id/details", g.GetBetDetails)
	g.Router.GET("/event/:id/bets", g.GetBetsByEvent)
	g.Router.GET("/events", g.GetEvents)
}

func (g *Gateway) Run(addr string) error {
	return g.Router.Run(addr)
}

func (g *Gateway) GetIndex(c *gin.Context) {
	c.HTML(http.StatusOK, "index.html", nil)
}

func (g *Gateway) GetEvents(c *gin.Context) {
	// Проверяем фильтр по статусу.
	statusFilter := c.Query("status")
	switch statusFilter {
	case "", "open", "win", "loss":
		// Допустимое значение.
	default:
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "invalid status: expected open, win or loss",
		})
		return
	}

	// Размер страницы по умолчанию — 20.
	limit := 20
	if rawLimit := c.Query("limit"); rawLimit != "" {
		parsedLimit, err := strconv.Atoi(rawLimit)
		if err != nil || parsedLimit < 1 || parsedLimit > 100 {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "limit must be between 1 and 100",
			})
			return
		}
		limit = parsedLimit
	}

	// Смещение по умолчанию — 0.
	offset := 0
	if rawOffset := c.Query("offset"); rawOffset != "" {
		parsedOffset, err := strconv.Atoi(rawOffset)
		if err != nil || parsedOffset < 0 {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "offset must be a non-negative integer",
			})
			return
		}
		offset = parsedOffset
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	result, status, err := g.BetClient.GetEvents(
		ctx,
		statusFilter,
		limit,
		offset,
	)

	if err != nil && status == "" {
		code := http.StatusBadGateway
		if errors.Is(err, context.DeadlineExceeded) {
			code = http.StatusGatewayTimeout
		}

		c.JSON(code, gin.H{"error": err.Error()})
		return
	}

	switch status {
	case "OK":
		if result == nil {
			c.JSON(http.StatusBadGateway, gin.H{
				"error": "events service returned an empty response",
			})
			return
		}
		c.JSON(http.StatusOK, result)

	case "VALIDATION_ERROR":
		message := "invalid events request"
		if err != nil {
			message = err.Error()
		}
		c.JSON(http.StatusBadRequest, gin.H{"error": message})

	default:
		message := "internal error"
		if err != nil {
			message = err.Error()
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": message})
	}
}

func (g *Gateway) GetBet(c *gin.Context) {
	rawID := c.Param("id")
	id, err := strconv.Atoi(rawID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "bad id"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	bet, status, err := g.BetClient.GetBet(ctx, id)
	if err != nil && status == "" {
		code := http.StatusBadGateway
		if errors.Is(err, context.DeadlineExceeded) {
			code = http.StatusGatewayTimeout
		}
		c.JSON(code, gin.H{"error": err.Error()})
		return
	}

	switch status {
	case "OK":
		c.JSON(http.StatusOK, bet)
	case "NOT_FOUND":
		c.JSON(http.StatusNotFound, gin.H{"error": "bet not found"})
	case "VALIDATION_ERROR":
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	default: // INTERNAL_ERROR и прочие.
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
	}
}

type CreateBetHTTPReq struct {
	UserID  int    `json:"user_id" binding:"required,min=1"`
	EventID int    `json:"event_id" binding:"required,min=1"`
	Amount  string `json:"amount" binding:"required"`
}

func (g *Gateway) CreateBet(c *gin.Context) {
	var httpReq CreateBetHTTPReq
	if err := c.ShouldBindJSON(&httpReq); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body: " + err.Error()})
		return
	}
	amount, err := decimal.NewFromString(httpReq.Amount)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid amount format, use string like '100.50'"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	bet, status, err := g.BetClient.CreateBet(ctx, httpReq.UserID, httpReq.EventID, amount)
	if err != nil && status == "" {
		code := http.StatusBadGateway
		if errors.Is(err, context.DeadlineExceeded) {
			code = http.StatusGatewayTimeout
		}
		c.JSON(code, gin.H{"error": err.Error()})
		return
	}

	switch status {
	case "OK":
		c.JSON(http.StatusOK, bet)
		return
	case "NOT_FOUND":
		c.JSON(http.StatusNotFound, gin.H{"error": "event not found"})
		return
	case "EVENT_CLOSED":
		c.JSON(http.StatusConflict, gin.H{"error": "событие закрыто или не принимает ставки"})
		return
	case "VALIDATION_ERROR":
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	default: // INTERNAL_ERROR и прочие.
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
}

func (g *Gateway) GetBetDetails(c *gin.Context) {
	id, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "bad id"})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	details, status, err := g.BetClient.GetBetDetails(ctx, id)
	if err != nil && status == "" {
		code := http.StatusBadGateway
		if errors.Is(err, context.DeadlineExceeded) {
			code = http.StatusGatewayTimeout
		}
		c.JSON(code, gin.H{"error": err.Error()})
		return
	}

	switch status {
	case "OK":
		c.JSON(http.StatusOK, details)
	case "NOT_FOUND":
		message := "bet or event not found"
		if err != nil {
			message = err.Error()
		}
		c.JSON(http.StatusNotFound, gin.H{"error": message})
	case "VALIDATION_ERROR":
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	default:
		message := "internal error"
		if err != nil {
			message = err.Error()
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": message})
	}
}

func (g *Gateway) GetBetsByEvent(c *gin.Context) {
	eventID, err := strconv.Atoi(c.Param("id"))
	if err != nil || eventID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "bad event id"})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	result, status, err := g.BetClient.GetBetsByEvent(ctx, eventID)

	if err != nil && status == "" {
		code := http.StatusBadGateway
		if errors.Is(err, context.DeadlineExceeded) {
			code = http.StatusGatewayTimeout
		}
		c.JSON(code, gin.H{"error": err.Error()})
		return
	}

	switch status {
	case "OK":
		c.JSON(http.StatusOK, result)

	case "VALIDATION_ERROR":
		message := "invalid event id"
		if err != nil {
			message = err.Error()
		}
		c.JSON(http.StatusBadRequest, gin.H{"error": message})

	default:
		message := "internal error"
		if err != nil {
			message = err.Error()
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": message})
	}
}
