package v1

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/usememos/memos/internal/webhook"
	v1pb "github.com/usememos/memos/proto/gen/api/v1"
	"github.com/usememos/memos/store"
)

// TestDispatchDeduplicatesRepeatedEvents verifies the persistent queue's
// idempotency guarantee through the real dispatch path: the same memo event
// dispatched twice produces exactly one HTTP delivery.
func TestDispatchDeduplicatesRepeatedEvents(t *testing.T) {
	ctx := context.Background()
	service := newIntegrationService(t)
	owner := createSpaceTestUser(ctx, t, service, "dedup-webhook-owner", store.RoleUser)
	ownerCtx := userCtx(ctx, owner.ID)

	var received atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		received.Add(1)
		_, _ = w.Write([]byte(`{"code":0}`))
	}))
	defer server.Close()

	host, _, err := net.SplitHostPort(server.Listener.Addr().String())
	require.NoError(t, err)
	require.NoError(t, webhook.ConfigurePrivateDestinationAllowlist([]string{host}))
	t.Cleanup(func() { require.NoError(t, webhook.ConfigurePrivateDestinationAllowlist(nil)) })

	_, err = service.CreateUserWebhook(ownerCtx, &v1pb.CreateUserWebhookRequest{
		Parent: BuildUserName(owner.Username),
		Webhook: &v1pb.UserWebhook{
			DisplayName: "dedup-capture",
			Url:         server.URL,
		},
	})
	require.NoError(t, err)

	memo := &v1pb.Memo{
		Name:       BuildUserName(owner.Username) + "/memos/memo-dedup",
		Creator:    BuildUserName(owner.Username),
		CreateTime: timestamppb.New(time.Now()),
	}
	require.NoError(t, service.DispatchMemoCreatedWebhook(ownerCtx, memo))
	// The same logical event triggered again must collapse onto the persisted row.
	require.NoError(t, service.DispatchMemoCreatedWebhook(ownerCtx, memo))

	require.Eventually(t, func() bool { return received.Load() == 1 }, 3*time.Second, 10*time.Millisecond)
	time.Sleep(200 * time.Millisecond)
	require.Equal(t, int32(1), received.Load(), "the repeated event must not be delivered twice")
}
