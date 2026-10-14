import { create } from "@bufbuild/protobuf";
import { FieldMaskSchema } from "@bufbuild/protobuf/wkt";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { attachmentServiceClient } from "@/connect";
import { userKeys } from "@/hooks/useUserQueries";
import {
  AttachmentQuotaSchema,
  GetAttachmentQuotaRequestSchema,
  ListAttachmentQuotasRequestSchema,
  UpdateAttachmentQuotaRequestSchema,
} from "@/types/proto/api/v1/attachment_service_pb";
import type { AttachmentQuota } from "@/types/proto/api/v1/attachment_service_pb";

export const attachmentQuotaKeys = {
  all: ["attachment-quota"] as const,
  list: () => [...attachmentQuotaKeys.all, "list"] as const,
  detail: (name: string) => [...attachmentQuotaKeys.all, "detail", name] as const,
};

// Per-user attachment quota of the signed-in user (or any user for admins).
export function useAttachmentQuota(name: string | undefined, options?: { enabled?: boolean }) {
  return useQuery({
    queryKey: attachmentQuotaKeys.detail(name ?? ""),
    queryFn: async () => {
      const { quota } = await attachmentServiceClient.getAttachmentQuota(create(GetAttachmentQuotaRequestSchema, { name }));
      return quota;
    },
    enabled: (options?.enabled ?? true) && !!name,
  });
}

// Admin-only list of every user's quota state.
export function useAttachmentQuotaList() {
  return useQuery({
    queryKey: attachmentQuotaKeys.list(),
    queryFn: async () => {
      const response = await attachmentServiceClient.listAttachmentQuotas(create(ListAttachmentQuotasRequestSchema, { pageSize: 1000 }));
      return response.attachmentQuotas;
    },
  });
}

// Admin-only per-user override update. limitBytes -1 clears the override.
export function useUpdateAttachmentQuota() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: async (vars: { name: string; limitBytes: bigint }) => {
      return attachmentServiceClient.updateAttachmentQuota(
        create(UpdateAttachmentQuotaRequestSchema, {
          attachmentQuota: create(AttachmentQuotaSchema, { name: vars.name, limitBytes: vars.limitBytes }),
          updateMask: create(FieldMaskSchema, { paths: ["limit_bytes"] }),
        }),
      );
    },
    onSuccess: (_response, vars) => {
      queryClient.invalidateQueries({ queryKey: attachmentQuotaKeys.list() });
      queryClient.invalidateQueries({ queryKey: attachmentQuotaKeys.detail(vars.name) });
      queryClient.invalidateQueries({ queryKey: userKeys.stats() });
    },
  });
}
