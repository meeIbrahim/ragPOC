import type { DocumentEntry } from "../../api/types";
import type { LoadState, PollState } from "../../hooks/useDocuments";
import type { LocalRow } from "../../hooks/useUploads";
import { MAX_SELECTED } from "../../limits";
import { Button } from "../ui/Button";
import { SpinnerIcon } from "../ui/icons";
import { Skeleton } from "../ui/Skeleton";
import { DocumentList } from "./DocumentList";
import { fromEntry, fromLocal } from "./rowModel";
import { UploadDropzone } from "./UploadDropzone";
import styles from "./DocumentsPanel.module.css";

interface Props {
  entries: DocumentEntry[];
  localRows: LocalRow[];
  load: LoadState;
  poll: PollState;
  truncated: boolean;
  now: number;
  selected: ReadonlySet<string>;
  onSelectionChange: (next: Set<string>) => void;
  /** Checking one more row past MAX_SELECTED (design 4). */
  onCapExceeded: () => void;
  onRefresh: () => void;
  uploadError: string | undefined;
  onFiles: (files: File[]) => void;
  onRetryUpload: (key: number) => void;
  onDismissUpload: (key: number) => void;
  flash: { id: string; n: number } | undefined;
}

export function DocumentsPanel(p: Props) {
  const rows = [...p.localRows.map(fromLocal), ...p.entries.map((e) => fromEntry(e, p.now))];

  const toggle = (id: string) => {
    const next = new Set(p.selected);
    if (next.has(id)) {
      next.delete(id);
    } else if (next.size >= MAX_SELECTED) {
      p.onCapExceeded();
      return;
    } else {
      next.add(id);
    }
    p.onSelectionChange(next);
  };

  let body;
  if (p.load === "loading") {
    body = (
      <div className={styles.skeletons} data-testid="documents-loading">
        {[0, 1, 2, 3].map((i) => (
          <Skeleton key={i} />
        ))}
      </div>
    );
  } else if (p.load === "failed") {
    body = (
      <div className={styles.message} role="alert">
        <p className={styles.title}>Couldn't reach the server.</p>
        <Button variant="secondary" onClick={p.onRefresh}>
          Try again
        </Button>
      </div>
    );
  } else if (rows.length === 0) {
    body = (
      <div className={styles.message}>
        <p className={styles.title}>No documents yet.</p>
        <p className={styles.hint}>Upload a PDF, TXT or MD file. Short files index fastest; long PDFs can take several minutes.</p>
      </div>
    );
  } else {
    body = (
      <>
        <DocumentList
          rows={rows}
          selected={p.selected}
          onToggle={toggle}
          onRetry={p.onRetryUpload}
          onDismiss={p.onDismissUpload}
          flash={p.flash}
        />
        {p.truncated && <p className={styles.note}>Showing the newest {p.entries.length} documents.</p>}
      </>
    );
  }

  return (
    <section className={styles.panel} aria-labelledby="documents-title">
      <h2 id="documents-title" className={styles.heading}>
        Documents
      </h2>
      <UploadDropzone error={p.uploadError} onFiles={p.onFiles} />
      <div className={styles.scroll}>
        {body}
        {p.load === "ready" && p.poll === "reconnecting" && (
          <p className={styles.note}>
            <SpinnerIcon /> Reconnecting…
          </p>
        )}
        {p.load === "ready" && p.poll === "stale" && (
          <p className={styles.note}>
            Can't reach the server. Status may be out of date.{" "}
            <Button variant="ghost" onClick={p.onRefresh}>
              Retry now
            </Button>
          </p>
        )}
      </div>
    </section>
  );
}
