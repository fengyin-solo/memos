package v1

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/usememos/memos/server/auth"
	"github.com/usememos/memos/server/webhookdelivery"
	"github.com/usememos/memos/store"
)

func setupWebhookDeliveryRouteTest(t *testing.T) (*APIV1Service, *echo.Echo, *store.User, *store.User) {
	t.Helper()
	ctx := context.Background()
	svc := newIntegrationService(t)
	// Deterministic queue state: stop the background workers for this test.
	svc.ShutdownWebhookDeliveryWorkers(ctx)

	host, err := svc.Store.CreateUser(ctx, &store.User{Username: "delivery-host", Role: store.RoleAdmin, Email: "delivery-host@example.com"})
	require.NoError(t, err)
	member, err := svc.Store.CreateUser(ctx, &store.User{Username: "delivery-member", Role: store.RoleUser, Email: "delivery-member@example.com"})
	require.NoError(t, err)

	router := echo.New()
	RegisterWebhookDeliveryRoutes(router, svc.Store, "test-secret", svc.WebhookQueue)
	return svc, router, host, member
}

func bearerToken(t *testing.T, user *store.User) string {
	t.Helper()
	token, _, err := auth.GenerateAccessTokenV2(user.ID, user.Username, string(user.Role), "ACTIVE", []byte("test-secret"))
	require.NoError(t, err)
	return "Bearer " + token
}

func performRouteRequest(t *testing.T, router *echo.Echo, method, path, token string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	if token != "" {
		req.Header.Set("Authorization", token)
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	var body map[string]any
	if rec.Body.Len() > 0 {
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	}
	return rec.Code, body
}

// TestWebhookDeliveryRoutesRequireHost covers authn/authz of the admin routes.
func TestWebhookDeliveryRoutesRequireHost(t *testing.T) {
	_, router, host, member := setupWebhookDeliveryRouteTest(t)

	status, _ := performRouteRequest(t, router, http.MethodGet, "/api/v1/webhook-deliveries/stats", "")
	require.Equal(t, http.StatusUnauthorized, status)

	status, _ = performRouteRequest(t, router, http.MethodGet, "/api/v1/webhook-deliveries/stats", bearerToken(t, member))
	require.Equal(t, http.StatusForbidden, status)

	status, _ = performRouteRequest(t, router, http.MethodGet, "/api/v1/webhook-deliveries/stats", bearerToken(t, host))
	require.Equal(t, http.StatusOK, status)
}

// TestWebhookDeliveryStatsRoute verifies the per-webhook pending/succeeded/
// failed/dead-letter counts, with PROCESSING folded into pending.
func TestWebhookDeliveryStatsRoute(t *testing.T) {
	svc, router, host, _ := setupWebhookDeliveryRouteTest(t)
	ctx := context.Background()

	insert := func(webhookID, eventKey string, status store.WebhookDeliveryStatus) int64 {
		delivery, _, err := svc.WebhookQueue.Enqueue(ctx, newRouteTestEnqueue(webhookID, eventKey))
		require.NoError(t, err)
		require.NoError(t, svc.Store.CompleteWebhookDelivery(ctx, &store.CompleteWebhookDelivery{
			ID:       delivery.ID,
			Status:   status,
			Attempts: 1,
		}))
		return delivery.ID
	}
	insert("hook-a", "a-1", store.WebhookDeliverySucceeded)
	insert("hook-a", "a-2", store.WebhookDeliveryFailed)
	insert("hook-a", "a-3", store.WebhookDeliveryDeadLetter)
	insert("hook-a", "a-4", store.WebhookDeliveryDeadLetter)
	pendingID := insert("hook-a", "a-5", store.WebhookDeliveryPending)
	require.NoError(t, svc.Store.CompleteWebhookDelivery(ctx, &store.CompleteWebhookDelivery{
		ID: pendingID, Status: store.WebhookDeliveryProcessing, Attempts: 1,
	}))
	insert("hook-b", "b-1", store.WebhookDeliverySucceeded)

	code, body := performRouteRequest(t, router, http.MethodGet, "/api/v1/webhook-deliveries/stats", bearerToken(t, host))
	require.Equal(t, http.StatusOK, code)
	webhooks, ok := body["webhooks"].([]any)
	require.True(t, ok)
	require.Len(t, webhooks, 2)
	byID := map[string]map[string]any{}
	for _, raw := range webhooks {
		entry, ok := raw.(map[string]any)
		require.True(t, ok)
		byID[entry["webhook_id"].(string)] = entry
	}
	require.EqualValues(t, 1, byID["hook-a"]["pending"], "PROCESSING counts as pending")
	require.EqualValues(t, 1, byID["hook-a"]["succeeded"])
	require.EqualValues(t, 1, byID["hook-a"]["failed"])
	require.EqualValues(t, 2, byID["hook-a"]["dead_letter"])
	require.EqualValues(t, 1, byID["hook-b"]["succeeded"])

	// Filter by webhook.
	code, body = performRouteRequest(t, router, http.MethodGet, "/api/v1/webhook-deliveries/stats?webhook_id=hook-b", bearerToken(t, host))
	require.Equal(t, http.StatusOK, code)
	filtered, ok := body["webhooks"].([]any)
	require.True(t, ok)
	require.Len(t, filtered, 1)
}

// TestWebhookDeliveryListAndDetailRoute verifies filtering, pagination, detail
// lookup, and that signing secrets never appear in responses.
func TestWebhookDeliveryListAndDetailRoute(t *testing.T) {
	svc, router, host, _ := setupWebhookDeliveryRouteTest(t)
	ctx := context.Background()

	deadID := int64(0)
	for i, status := range []store.WebhookDeliveryStatus{
		store.WebhookDeliverySucceeded,
		store.WebhookDeliveryFailed,
		store.WebhookDeliveryDeadLetter,
	} {
		delivery, _, err := svc.WebhookQueue.Enqueue(ctx, newRouteTestEnqueue("hook-a", "list-"+string(rune('a'+i))))
		require.NoError(t, err)
		require.NoError(t, svc.Store.CompleteWebhookDelivery(ctx, &store.CompleteWebhookDelivery{
			ID: delivery.ID, Status: status, Attempts: 2, LastStatusCode: 500, LastError: "boom",
		}))
		if status == store.WebhookDeliveryDeadLetter {
			deadID = delivery.ID
		}
	}

	code, body := performRouteRequest(t, router, http.MethodGet, "/api/v1/webhook-deliveries?status=DEAD_LETTER", bearerToken(t, host))
	require.Equal(t, http.StatusOK, code)
	deliveries, ok := body["deliveries"].([]any)
	require.True(t, ok)
	require.Len(t, deliveries, 1)
	entry, ok := deliveries[0].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "DEAD_LETTER", entry["status"])
	require.NotEmpty(t, entry["idempotency_key"])
	require.Equal(t, "http://example.com/hook", entry["url"])
	require.Equal(t, "hook-a", entry["webhook_id"])
	require.Equal(t, map[string]any{"hello": "world"}, entry["payload"])
	require.NotContains(t, entry, "signing_secret", "signing secrets must never be exposed")

	// Bad filter and pagination.
	code, _ = performRouteRequest(t, router, http.MethodGet, "/api/v1/webhook-deliveries?status=NOPE", bearerToken(t, host))
	require.Equal(t, http.StatusBadRequest, code)
	code, _ = performRouteRequest(t, router, http.MethodGet, "/api/v1/webhook-deliveries?limit=-1", bearerToken(t, host))
	require.Equal(t, http.StatusBadRequest, code)

	// Detail.
	code, body = performRouteRequest(t, router, http.MethodGet, "/api/v1/webhook-deliveries/"+itoa(deadID), bearerToken(t, host))
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, "boom", body["last_error"])
	require.EqualValues(t, 500, body["last_status_code"])

	code, _ = performRouteRequest(t, router, http.MethodGet, "/api/v1/webhook-deliveries/not-a-number", bearerToken(t, host))
	require.Equal(t, http.StatusBadRequest, code)
	code, _ = performRouteRequest(t, router, http.MethodGet, "/api/v1/webhook-deliveries/99999999", bearerToken(t, host))
	require.Equal(t, http.StatusNotFound, code)
}

// TestWebhookDeliveryReplayRoute verifies read-only replay of a dead-letter
// entry: the original is untouched and a fresh PENDING delivery is returned.
func TestWebhookDeliveryReplayRoute(t *testing.T) {
	svc, router, host, _ := setupWebhookDeliveryRouteTest(t)
	ctx := context.Background()

	dead, _, err := svc.WebhookQueue.Enqueue(ctx, newRouteTestEnqueue("hook-a", "replay-dead"))
	require.NoError(t, err)
	require.NoError(t, svc.Store.CompleteWebhookDelivery(ctx, &store.CompleteWebhookDelivery{
		ID: dead.ID, Status: store.WebhookDeliveryDeadLetter, Attempts: 5, LastStatusCode: 500,
	}))
	failed, _, err := svc.WebhookQueue.Enqueue(ctx, newRouteTestEnqueue("hook-a", "replay-failed"))
	require.NoError(t, err)
	require.NoError(t, svc.Store.CompleteWebhookDelivery(ctx, &store.CompleteWebhookDelivery{
		ID: failed.ID, Status: store.WebhookDeliveryFailed, Attempts: 1, LastStatusCode: 404,
	}))

	code, body := performRouteRequest(t, router, http.MethodPost, "/api/v1/webhook-deliveries/"+itoa(dead.ID)+"/replay", bearerToken(t, host))
	require.Equal(t, http.StatusOK, code)
	require.NotEqual(t, float64(dead.ID), body["id"])
	require.Equal(t, "PENDING", body["status"])
	require.Equal(t, "hook-a", body["webhook_id"])

	// The original dead-letter row is untouched.
	original, err := findRouteTestDelivery(svc, dead.ID)
	require.NoError(t, err)
	require.Equal(t, store.WebhookDeliveryDeadLetter, original.Status)
	require.Equal(t, int32(5), original.Attempts)

	// Only dead letters can be replayed.
	code, _ = performRouteRequest(t, router, http.MethodPost, "/api/v1/webhook-deliveries/"+itoa(failed.ID)+"/replay", bearerToken(t, host))
	require.Equal(t, http.StatusConflict, code)
	code, _ = performRouteRequest(t, router, http.MethodPost, "/api/v1/webhook-deliveries/99999999/replay", bearerToken(t, host))
	require.Equal(t, http.StatusNotFound, code)
	code, _ = performRouteRequest(t, router, http.MethodPost, "/api/v1/webhook-deliveries/bad/replay", bearerToken(t, host))
	require.Equal(t, http.StatusBadRequest, code)
}

func newRouteTestEnqueue(webhookID, eventKey string) *webhookdelivery.EnqueueRequest {
	return &webhookdelivery.EnqueueRequest{
		EventKey:  eventKey,
		WebhookID: webhookID,
		UserID:    1,
		URL:       "http://example.com/hook",
		Label:     "memos.memo.created",
		Payload:   map[string]any{"hello": "world"},
	}
}

func findRouteTestDelivery(svc *APIV1Service, id int64) (*store.WebhookDelivery, error) {
	rows, err := svc.Store.ListWebhookDeliveries(context.Background(), &store.FindWebhookDelivery{ID: &id})
	if err != nil {
		return nil, err
	}
	if len(rows) != 1 {
		return nil, errWebhookDeliveryRouteNotFound
	}
	return rows[0], nil
}

var errWebhookDeliveryRouteNotFound = errors.New("webhook delivery not found")

func itoa(id int64) string {
	return strconv.FormatInt(id, 10)
}
