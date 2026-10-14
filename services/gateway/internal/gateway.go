package gateway

import (
	bets "bookmaker/bets"
	"bookmaker/pkg/rmq"
	"bookmaker/shared/dto"
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/shopspring/decimal"
)

type Gateway struct {
	BetClient *bets.BetClient
	Router    *gin.Engine
}

func (g *Gateway) SetupRouter() {
	// TODO: когда будет больше роутов нужно будет сформировать группы
	g.Router.LoadHTMLGlob("/app/templates/*.html")
	g.Router.GET("/", g.GetIndex)
	g.Router.GET("/bet/:id", g.GetBet)
	g.Router.POST("/bet", g.CreateBet)
}

func (g *Gateway) Run(addr string) error {
	return g.Router.Run(addr)
}

func (g *Gateway) GetIndex(c *gin.Context) {
	c.HTML(http.StatusOK, "index.html", nil)
}

func (g *Gateway) GetBet(c *gin.Context) {
	rawId := c.Param("id")
	id, err := strconv.Atoi(rawId)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "bad id"})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second) // TODO: надо вынести в параметр
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
	case rmq.STATUS_OK:
		c.JSON(http.StatusOK, bet)
	case dto.STATUS_NOT_FOUND:
		c.JSON(http.StatusNotFound, gin.H{"error": "bet not found"})
	case rmq.STATUS_ERROR:
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
	default:
		c.JSON(http.StatusInternalServerError, gin.H{"error": "undefined status: " + status})
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
	case rmq.STATUS_OK:
		c.JSON(http.StatusOK, bet)
		return
	case dto.STATUS_NOT_FOUND:
		c.JSON(http.StatusNotFound, gin.H{"error": "bet not found"})
		return
	case rmq.STATUS_ERROR:
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	case dto.STATUS_EVENT_CLOSED:
		c.JSON(http.StatusConflict, gin.H{"error": "событие закрыто или не принимает ставки"})
		return
	default:
		c.JSON(http.StatusInternalServerError, gin.H{"error": "undefined status: " + status})
		return
	}
}
