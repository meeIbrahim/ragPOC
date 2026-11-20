import type { ReactNode } from "react";
import { CheckIcon, ClockIcon, CrossIcon, SpinnerIcon } from "./icons";
import styles from "./StatusBadge.module.css";

export type BadgeStatus = "queued" | "uploading" | "processing" | "indexed" | "failed";

const look: Record<BadgeStatus, { label: string; icon: ReactNode; tone: string }> = {
  queued: { label: "Queued", icon: <ClockIcon />, tone: "queued" },
  uploading: { label: "Uploading", icon: <SpinnerIcon />, tone: "info" },
  processing: { label: "Processing", icon: <SpinnerIcon />, tone: "info" },
  indexed: { label: "Indexed", icon: <CheckIcon />, tone: "success" },
  failed: { label: "Failed", icon: <CrossIcon />, tone: "error" },
};

/** Icon plus text plus color: status is never shown by color alone (tokens.md). */
export function StatusBadge({ status }: { status: BadgeStatus }) {
  const { label, icon, tone } = look[status];
  return (
    <span className={`${styles.badge} ${styles[tone] ?? ""}`}>
      {icon}
      {label}
    </span>
  );
}
