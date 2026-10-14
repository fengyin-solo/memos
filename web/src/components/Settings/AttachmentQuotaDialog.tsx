import { useEffect, useState } from "react";
import toast from "react-hot-toast";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useUpdateAttachmentQuota } from "@/hooks/useAttachmentQuota";
import { handleError } from "@/lib/error";
import type { AttachmentQuota } from "@/types/proto/api/v1/attachment_service_pb";
import { formatFileSize } from "@/utils/format";
import { useTranslate } from "@/utils/i18n";

interface Props {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  quota?: AttachmentQuota;
  username: string;
}

const BYTES_PER_MIB = 1024 * 1024;

const AttachmentQuotaDialog = ({ open, onOpenChange, quota, username }: Props) => {
  const t = useTranslate();
  const { mutateAsync: updateQuota, isPending } = useUpdateAttachmentQuota();
  const [mib, setMib] = useState("100");
  const [inherit, setInherit] = useState(false);

  useEffect(() => {
    if (!open) return;
    setInherit(!quota?.limitOverridden);
    setMib(quota && quota.limitBytes >= 0 ? String(Math.max(1, Math.round(Number(quota.limitBytes) / BYTES_PER_MIB))) : "100");
  }, [open, quota]);

  const quotaName = `users/${username}/attachmentQuota`;
  const parsedMib = parseInt(mib, 10);
  const valid = inherit || (Number.isFinite(parsedMib) && parsedMib >= 0);

  const handleSave = async () => {
    try {
      await updateQuota({
        name: quotaName,
        limitBytes: inherit ? -1n : BigInt(Math.round(parsedMib * BYTES_PER_MIB)),
      });
      toast.success(t("setting.member.quota-update-success", { username }));
      onOpenChange(false);
    } catch (error) {
      handleError(error, toast.error, { context: "Update attachment quota" });
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>{t("setting.member.quota-dialog-title", { username })}</DialogTitle>
        </DialogHeader>
        <div className="flex flex-col gap-4 py-2">
          {quota ? (
            <p className="text-sm text-muted-foreground">
              {t("setting.member.quota-current-usage", {
                used: formatFileSize(Number(quota.usedBytes)),
                limit: quota.enabled ? formatFileSize(Number(quota.limitBytes)) : t("setting.member.quota-unlimited"),
              })}
            </p>
          ) : null}
          <div className="flex items-center gap-2">
            <input
              id="quota-inherit"
              type="checkbox"
              className="size-4 rounded border-input"
              checked={inherit}
              onChange={(e) => setInherit(e.target.checked)}
            />
            <Label htmlFor="quota-inherit" className="font-normal">
              {t("setting.member.quota-inherit-default")}
            </Label>
          </div>
          {!inherit ? (
            <div className="flex flex-col gap-2">
              <Label htmlFor="quota-mib">{t("setting.member.quota-custom-limit")}</Label>
              <div className="flex items-center gap-2">
                <Input id="quota-mib" className="w-28 font-mono" value={mib} onChange={(e) => setMib(e.target.value)} />
                <span className="text-sm text-muted-foreground">MiB</span>
              </div>
              <p className="text-xs text-muted-foreground">{t("setting.member.quota-zero-blocks")}</p>
            </div>
          ) : null}
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            {t("common.cancel")}
          </Button>
          <Button disabled={!valid || isPending} onClick={handleSave}>
            {t("common.save")}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
};

export default AttachmentQuotaDialog;
