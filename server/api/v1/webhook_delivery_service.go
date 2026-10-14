package v1

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
	"github.com/pkg/errors"

	"github.com/usememos/memos/server/auth"
	"github.com/usememos/memos/server/webhookdelivery"
	"github.com/usememos/memos/store"
)

// webhookDeliveryRouter is the subset of the Echo router the delivery routes need.
type webhookDeliveryRouter interface {
	GET(path string, h echo.HandlerFunc, m ...echo.MiddlewareFunc) echo.RouteInfo
	POST(path string, h echo.HandlerFunc, m ...echo.MiddlewareFunc) echo.RouteInfo
}

const (
	defaultWebhookDeliveryListLimit = 50
	maxWebhookDeliveryListLimit     = 200
)

// RegisterWebhookDeliveryRoutes registers the administrator-only observability
// and replay routes for the persistent outbound webhook queue. These are native
// HTTP routes (not gRPC/Connect procedures) and enforce authentication and host
// authorization themselves.
func RegisterWebhookDeliveryRoutes(router webhookDeliveryRouter, storeInstance *store.Store, secret string, queue *webhookdelivery.Queue) {
	authenticator := auth.NewAuthenticator(storeInstance, secret)
	requireAdmin := func(c *echo.Context) bool {
		return requireWebhookDeliveryAdmin(c, storeInstance, authenticator)
	}

	router.GET("/api/v1/webhook-deliveries/stats", func(c *echo.Context) error {
		if !requireAdmin(c) {
			return nil
		}
		return listWebhookDeliveryStats(c, storeInstance)
	})
	router.GET("/api/v1/webhook-deliveries", func(c *echo.Context) error {
		if !requireAdmin(c) {
			return nil
		}
		return listWebhookDeliveries(c, storeInstance)
	})
	router.GET("/api/v1/webhook-deliveries/:id", func(c *echo.Context) error {
		if !requireAdmin(c) {
			return nil
		}
		return getWebhookDelivery(c, storeInstance)
	})
	router.POST("/api/v1/webhook-deliveries/:id/replay", func(c *echo.Context) error {
		if !requireAdmin(c) {
			return nil
		}
		if queue == nil {
			return c.JSON(http.StatusServiceUnavailable, map[string]string{"error": "webhook delivery queue is unavailable"})
		}
		return replayWebhookDelivery(c, queue)
	})
}

// requireWebhookDeliveryAdmin authenticates the request like the SSE route and
// additionally requires the host role. It writes the error response itself and
// returns false so the caller stops handling the request.
func requireWebhookDeliveryAdmin(c *echo.Context, storeInstance *store.Store, authenticator *auth.Authenticator) bool {
	result := authenticator.Authenticate(c.Request().Context(), c.Request().Header.Get("Authorization"))
	userID := getSSEClientUserID(result)
	if userID == 0 {
		_ = c.JSON(http.StatusUnauthorized, map[string]string{"error": "authentication required"})
		return false
	}
	user, err := storeInstance.GetUser(c.Request().Context(), &store.FindUser{ID: &userID})
	if err != nil || user == nil {
		_ = c.JSON(http.StatusUnauthorized, map[string]string{"error": "authentication required"})
		return false
	}
	if user.Role != store.RoleAdmin {
		_ = c.JSON(http.StatusForbidden, map[string]string{"error": "administrator permission required"})
		return false
	}
	return true
}

// webhookDeliveryResponse is the JSON representation of one queued delivery.
// The signing secret is intentionally never exposed.
type webhookDeliveryResponse struct {
	ID             int64           `json:"id"`
	IdempotencyKey string          `json:"idempotency_key"`
	WebhookID      string          `json:"webhook_id"`
	UserID         int32           `json:"user_id"`
	URL            string          `json:"url"`
	Label          string          `json:"label"`
	MessageID      string          `json:"message_id"`
	Status         string          `json:"status"`
	Attempts       int32           `json:"attempts"`
	MaxAttempts    int32           `json:"max_attempts"`
	CreatedTs      int64           `json:"created_ts"`
	UpdatedTs      int64           `json:"updated_ts"`
	NextAttemptTs  int64           `json:"next_attempt_ts"`
	SucceededTs    int64           `json:"succeeded_ts"`
	LastStatusCode int32           `json:"last_status_code"`
	LastError      string          `json:"last_error"`
	Payload        json.RawMessage `json:"payload"`
}

// webhookDeliveryStatsResponse reports delivery counts for one webhook.
// Pending counts both waiting PENDING rows and in-flight PROCESSING rows.
type webhookDeliveryStatsResponse struct {
	WebhookID  string `json:"webhook_id"`
	UserID     int32  `json:"user_id"`
	Pending    int64  `json:"pending"`
	Succeeded  int64  `json:"succeeded"`
	Failed     int64  `json:"failed"`
	DeadLetter int64  `json:"dead_letter"`
}

func newWebhookDeliveryResponse(delivery *store.WebhookDelivery) webhookDeliveryResponse {
	payload := json.RawMessage(delivery.Payload)
	if len(payload) == 0 {
		payload = json.RawMessage(`null`)
	}
	return webhookDeliveryResponse{
		ID:             delivery.ID,
		IdempotencyKey: delivery.IdempotencyKey,
		WebhookID:      delivery.WebhookID,
		UserID:         delivery.UserID,
		URL:            delivery.URL,
		Label:          delivery.Label,
		MessageID:      delivery.MessageID,
		Status:         string(delivery.Status),
		Attempts:       delivery.Attempts,
		MaxAttempts:    delivery.MaxAttempts,
		CreatedTs:      delivery.CreatedTs,
		UpdatedTs:      delivery.UpdatedTs,
		NextAttemptTs:  delivery.NextAttemptTs,
		SucceededTs:    delivery.SucceededTs,
		LastStatusCode: delivery.LastStatusCode,
		LastError:      delivery.LastError,
		Payload:        payload,
	}
}

func listWebhookDeliveries(c *echo.Context, storeInstance *store.Store) error {
	find := &store.FindWebhookDelivery{}

	if webhookID := strings.TrimSpace(c.QueryParam("webhook_id")); webhookID != "" {
		find.WebhookID = &webhookID
	}
	if status := strings.TrimSpace(c.QueryParam("status")); status != "" {
		parsed := store.WebhookDeliveryStatus(status)
		switch parsed {
		case store.WebhookDeliveryPending, store.WebhookDeliveryProcessing, store.WebhookDeliverySucceeded, store.WebhookDeliveryFailed, store.WebhookDeliveryDeadLetter:
		default:
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid status filter"})
		}
		find.Status = &parsed
	}

	limit := defaultWebhookDeliveryListLimit
	if raw := strings.TrimSpace(c.QueryParam("limit")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid limit"})
		}
		if parsed > maxWebhookDeliveryListLimit {
			parsed = maxWebhookDeliveryListLimit
		}
		limit = parsed
	}
	find.Limit = &limit
	if raw := strings.TrimSpace(c.QueryParam("offset")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid offset"})
		}
		find.Offset = &parsed
	}

	deliveries, err := storeInstance.ListWebhookDeliveries(c.Request().Context(), find)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to list webhook deliveries"})
	}
	response := make([]webhookDeliveryResponse, 0, len(deliveries))
	for _, delivery := range deliveries {
		response = append(response, newWebhookDeliveryResponse(delivery))
	}
	return c.JSON(http.StatusOK, map[string]any{"deliveries": response})
}

func getWebhookDelivery(c *echo.Context, storeInstance *store.Store) error {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid webhook delivery id"})
	}
	deliveries, err := storeInstance.ListWebhookDeliveries(c.Request().Context(), &store.FindWebhookDelivery{ID: &id})
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to get webhook delivery"})
	}
	if len(deliveries) != 1 {
		return c.JSON(http.StatusNotFound, map[string]string{"error": "webhook delivery not found"})
	}
	return c.JSON(http.StatusOK, newWebhookDeliveryResponse(deliveries[0]))
}

func listWebhookDeliveryStats(c *echo.Context, storeInstance *store.Store) error {
	var webhookID *string
	if raw := strings.TrimSpace(c.QueryParam("webhook_id")); raw != "" {
		webhookID = &raw
	}
	counts, err := storeInstance.CountWebhookDeliveriesGrouped(c.Request().Context(), webhookID)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to count webhook deliveries"})
	}

	byWebhook := map[string]*webhookDeliveryStatsResponse{}
	for _, count := range counts {
		stats := byWebhook[count.WebhookID]
		if stats == nil {
			stats = &webhookDeliveryStatsResponse{WebhookID: count.WebhookID, UserID: count.UserID}
			byWebhook[count.WebhookID] = stats
		}
		switch count.Status {
		case store.WebhookDeliveryPending, store.WebhookDeliveryProcessing:
			stats.Pending += count.Count
		case store.WebhookDeliverySucceeded:
			stats.Succeeded += count.Count
		case store.WebhookDeliveryFailed:
			stats.Failed += count.Count
		case store.WebhookDeliveryDeadLetter:
			stats.DeadLetter += count.Count
		default:
			// Unknown statuses are ignored so future states cannot inflate
			// the pending count.
		}
	}

	response := make([]webhookDeliveryStatsResponse, 0, len(byWebhook))
	for _, stats := range byWebhook {
		response = append(response, *stats)
	}
	return c.JSON(http.StatusOK, map[string]any{"webhooks": response})
}

func replayWebhookDelivery(c *echo.Context, queue *webhookdelivery.Queue) error {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid webhook delivery id"})
	}
	delivery, err := queue.ReplayDeadLetter(c.Request().Context(), id)
	switch {
	case errors.Is(err, webhookdelivery.ErrDeliveryNotFound):
		return c.JSON(http.StatusNotFound, map[string]string{"error": "webhook delivery not found"})
	case errors.Is(err, webhookdelivery.ErrDeliveryNotDeadLetter):
		return c.JSON(http.StatusConflict, map[string]string{"error": "only dead letter deliveries can be replayed"})
	case err != nil:
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to replay webhook delivery"})
	}
	return c.JSON(http.StatusOK, newWebhookDeliveryResponse(delivery))
}
