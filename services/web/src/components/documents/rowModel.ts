import type { DocumentEntry } from "../../api/types";
import type { LocalRow } from "../../hooks/useUploads";
import type { BadgeStatus } from "../ui/StatusBadge";
import { reasonText } from "../../lib/reasons";
import { formatAbsolute, formatElapsed, formatRelative, isSlow } from "../../lib/time";

/** One list row, local or server, ready to render (design 3.2). */
export interface RowModel {
  domId: string;
  /** Entry id for server rows; undefined for local rows. */
  entryId: string | undefined;
  localKey: number | undefined;
  filename: string;
  badge: BadgeStatus;
  detail: string;
  /** title attribute for the detail (absolute time, or the raw failure code). */
  detailTitle: string | undefined;
  detailTone: "mono" | "muted" | "reason" | "plain";
  selectable: boolean;
  /** Failed before confirm: offers Retry and Dismiss. */
  retryable: boolean;
}

const badgeLabel: Record<BadgeStatus, string> = {
  queued: "Queued",
  uploading: "Uploading",
  processing: "Processing",
  indexed: "Indexed",
  failed: "Failed",
};

export function accessibleName(r: RowModel): string {
  return r.detail ? `${r.filename}, ${badgeLabel[r.badge]}: ${r.detail}` : `${r.filename}, ${badgeLabel[r.badge]}`;
}

export function fromLocal(r: LocalRow): RowModel {
  const base = { domId: `local-${r.key}`, entryId: undefined, localKey: r.key, filename: r.filename, selectable: false };
  switch (r.phase) {
    case "queued":
      return {
        ...base,
        badge: "queued",
        detail: `Waiting for ${r.ahead} ${r.ahead === 1 ? "upload" : "uploads"}`,
        detailTitle: undefined,
        detailTone: "plain",
        retryable: false,
      };
    case "uploading":
      return {
        ...base,
        badge: "uploading",
        detail: `${Math.round(r.progress * 100)}%`,
        detailTitle: undefined,
        detailTone: "mono",
        retryable: false,
      };
    case "processing":
      return {
        ...base,
        badge: "processing",
        detail: r.notListed ? "Taking longer than expected" : "",
        detailTitle: undefined,
        detailTone: "muted",
        retryable: false,
      };
    default:
      return {
        ...base,
        badge: "failed",
        detail: "Upload didn't finish. Try again.",
        detailTitle: undefined,
        detailTone: "reason",
        retryable: true,
      };
  }
}

export function fromEntry(e: DocumentEntry, now: number): RowModel {
  const base = { domId: `doc-${e.id}`, entryId: e.id, localKey: undefined, filename: e.filename, retryable: false };
  const age = now - e.createdAt;
  switch (e.status) {
    case "INDEXED":
      return {
        ...base,
        badge: "indexed",
        detail: formatRelative(age),
        detailTitle: formatAbsolute(e.createdAt),
        detailTone: "plain",
        selectable: true,
      };
    case "FAILED":
      return {
        ...base,
        badge: "failed",
        detail: reasonText(e.failureReason),
        detailTitle: e.failureReason,
        detailTone: "reason",
        selectable: false,
      };
    default:
      return {
        ...base,
        badge: "processing",
        detail: isSlow(age) ? "Taking longer than usual" : formatElapsed(age),
        detailTitle: undefined,
        detailTone: isSlow(age) ? "muted" : "plain",
        selectable: false,
      };
  }
}
