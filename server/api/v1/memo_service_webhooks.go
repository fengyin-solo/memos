package v1

import (
	"context"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/usememos/memos/core/access"
	v1pb "github.com/usememos/memos/proto/gen/api/v1"
	storepb "github.com/usememos/memos/proto/gen/store"
	"github.com/usememos/memos/server/auth"
	"github.com/usememos/memos/server/webhookdelivery"
	"github.com/usememos/memos/store"
)

// DispatchMemoCreatedWebhook dispatches a webhook when a memo is created.
func (s *APIV1Service) DispatchMemoCreatedWebhook(ctx context.Context, memo *v1pb.Memo) error {
	return s.dispatchMemoRelatedWebhook(ctx, memo, "memos.memo.created")
}

// DispatchMemoUpdatedWebhook dispatches webhook when memo is updated.
func (s *APIV1Service) DispatchMemoUpdatedWebhook(ctx context.Context, memo *v1pb.Memo) error {
	return s.dispatchMemoRelatedWebhook(ctx, memo, "memos.memo.updated")
}

// DispatchMemoDeletedWebhook dispatches webhook when memo is deleted.
func (s *APIV1Service) DispatchMemoDeletedWebhook(ctx context.Context, memo *v1pb.Memo) error {
	return s.dispatchMemoRelatedWebhook(ctx, memo, "memos.memo.deleted")
}

// DispatchMemoCommentCreatedWebhook dispatches webhook to the related memo owner when a comment is created.
func (s *APIV1Service) DispatchMemoCommentCreatedWebhook(
	ctx context.Context,
	comment, relatedMemo *store.Memo,
	relatedMemoCreatorID int32,
) error {
	receiver, err := s.Store.GetUser(ctx, &store.FindUser{ID: &relatedMemoCreatorID})
	if err != nil {
		return err
	}
	if receiver == nil || receiver.RowStatus != store.Normal {
		return nil
	}
	for _, subject := range []*store.Memo{comment, relatedMemo} {
		readContext, err := s.buildMemoReadContextForViewer(ctx, subject, receiver, false, nil)
		if err != nil {
			return errors.Wrap(err, "failed to resolve webhook subject access")
		}
		decision := access.CheckMemoReadContext(readContext)
		if !decision.Allowed() {
			return nil
		}
	}
	// The request context belongs to the comment author, but the webhook belongs
	// to the context memo's author. Build the prepared payload under the receiver's
	// identity before it enters the existing asynchronous delivery queue.
	receiverCtx := auth.SetUserInContext(ctx, receiver, "")
	reactions, err := s.Store.ListReactions(receiverCtx, &store.FindReaction{MemoID: &comment.ID})
	if err != nil {
		return err
	}
	attachments, err := s.Store.ListAttachments(receiverCtx, &store.FindAttachment{MemoID: &comment.ID})
	if err != nil {
		return err
	}
	relations, err := s.loadMemoRelations(receiverCtx, comment)
	if err != nil {
		return err
	}
	commentMessage, err := s.convertMemoFromStore(receiverCtx, comment, reactions, attachments, relations)
	if err != nil {
		return err
	}
	webhooks, err := s.Store.GetUserWebhooks(ctx, relatedMemoCreatorID)
	if err != nil {
		return err
	}
	for _, hook := range webhooks {
		if err := s.enqueueMemoWebhook(ctx, hook, relatedMemoCreatorID, "memos.memo.comment.created", commentMessage); err != nil {
			return err
		}
	}
	return nil
}

func (s *APIV1Service) dispatchMemoRelatedWebhook(ctx context.Context, memo *v1pb.Memo, activityType string) error {
	creator, err := ResolveUserByName(ctx, s.Store, memo.Creator)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid memo creator")
	}
	if creator == nil {
		return status.Errorf(codes.NotFound, "memo creator not found")
	}
	creatorID := creator.ID
	webhooks, err := s.Store.GetUserWebhooks(ctx, creatorID)
	if err != nil {
		return err
	}
	for _, hook := range webhooks {
		if err := s.enqueueMemoWebhook(ctx, hook, creatorID, activityType, memo); err != nil {
			return err
		}
	}
	return nil
}

// enqueueMemoWebhook persists one memo activity delivery for one configured
// webhook. The event key is deterministic from (activity, memo revision), so a
// repeated trigger of the same event collapses onto the existing delivery
// instead of being delivered twice.
func (s *APIV1Service) enqueueMemoWebhook(ctx context.Context, hook *storepb.WebhooksUserSetting_Webhook, creatorID int32, activityType string, memo *v1pb.Memo) error {
	if s.WebhookQueue == nil {
		return nil
	}
	_, _, err := s.WebhookQueue.Enqueue(ctx, &webhookdelivery.EnqueueRequest{
		EventKey:      memoWebhookEventKey(activityType, memo),
		WebhookID:     hook.Id,
		UserID:        creatorID,
		URL:           hook.Url,
		Label:         activityType,
		SigningSecret: hook.SigningSecret,
		Payload: &memoWebhookPayload{
			URL:          hook.Url,
			ActivityType: activityType,
			Creator:      memo.Creator,
			Memo:         memo,
		},
	})
	return err
}

// memoWebhookEventKey derives the deterministic logical-event identity. Create
// events key on the creation timestamp; updates, deletes, and comments on the
// last update timestamp, so distinct revisions produce distinct events while
// the same event fired twice produces only one delivery. Nanosecond precision
// keeps two operations landing in the same second from collapsing together.
func memoWebhookEventKey(activityType string, memo *v1pb.Memo) string {
	var eventTs int64
	if strings.HasSuffix(activityType, ".created") {
		if memo.CreateTime != nil {
			eventTs = memo.CreateTime.AsTime().UnixNano()
		}
	} else if memo.UpdateTime != nil {
		eventTs = memo.UpdateTime.AsTime().UnixNano()
	}
	return fmt.Sprintf("%s|%s|%d", activityType, memo.Name, eventTs)
}

// memoWebhookPayload is the JSON body delivered to user webhooks for memo
// activity. Field names are part of the webhook contract.
type memoWebhookPayload struct {
	// URL is the destination the payload was sent to.
	URL string `json:"url"`
	// ActivityType names the event, for example "memos.memo.created".
	ActivityType string `json:"activityType"`
	// Creator is the resource name of the memo creator. Format: users/{user}
	Creator string `json:"creator"`
	// Memo is the memo that triggered the event.
	Memo *v1pb.Memo `json:"memo"`
}

func (s *APIV1Service) getMemoContentSnippet(content string) (string, error) {
	// Use goldmark service for snippet generation
	snippet, err := s.MarkdownService.GenerateSnippet([]byte(content), 64)
	if err != nil {
		return "", errors.Wrap(err, "failed to generate snippet")
	}
	return snippet, nil
}

// parseMemoOrderBy parses the order_by field and sets the appropriate ordering in memoFind.
// Follows AIP-132: supports comma-separated list of fields with optional "desc" suffix.
// Example: "pinned desc, create_time desc" or "update_time asc".
