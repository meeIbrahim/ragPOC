import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { DocumentEntry } from "./api/types";
import { AskPanel } from "./components/ask/AskPanel";
import type { DocsReadiness } from "./components/ask/AnswerArea";
import { DocumentsPanel } from "./components/documents/DocumentsPanel";
import { InfoIcon } from "./components/ui/icons";
import { LiveRegionProvider, useAnnounce } from "./components/ui/LiveRegion";
import { ToastProvider, useToast } from "./components/ui/Toast";
import type { AskStreamFn } from "./hooks/useAsk";
import { useDocuments } from "./hooks/useDocuments";
import { useNow } from "./hooks/useNow";
import { useUploads, type Merge } from "./hooks/useUploads";
import { MAX_SELECTED, NOW_TICK_MS } from "./limits";
import { reasonText } from "./lib/reasons";
import { suggestions } from "./lib/suggestions";
import styles from "./App.module.css";

const SECOND = 1000;

function Name({ children }: { children: string }) {
  return <span dir="auto">{children}</span>;
}

function Page({ stream }: { stream: AskStreamFn | undefined }) {
  const toast = useToast();
  const announce = useAnnounce();
  const [uploadsActive, setUploadsActive] = useState(false);
  const docs = useDocuments(uploadsActive);
  const [selected, setSelected] = useState<Set<string>>(() => new Set());
  const [capError, setCapError] = useState<string | undefined>(undefined);
  const [flash, setFlash] = useState<{ id: string; n: number } | undefined>(undefined);

  const onMerged = useCallback(
    ({ filename, entry }: Merge) => {
      if (entry.status === "FAILED") {
        toast({
          kind: "error",
          message: (
            <>
              "<Name>{filename}</Name>" matches "<Name>{entry.filename}</Name>", which failed: {reasonText(entry.failureReason)} Uploading
              it again won't retry it.
            </>
          ),
        });
      } else {
        toast({
          kind: "info",
          message: (
            <>
              "<Name>{filename}</Name>" is already uploaded as "<Name>{entry.filename}</Name>".
            </>
          ),
        });
      }
      setFlash((f) => ({ id: entry.id, n: (f?.n ?? 0) + 1 }));
    },
    [toast],
  );

  const uploads = useUploads({ entries: docs.entries, version: docs.version, onConfirmed: docs.refresh, onMerged });
  useEffect(() => setUploadsActive(uploads.active), [uploads.active]);

  const processing = docs.entries.some((e) => e.status === "PROCESSING") || uploads.rows.some((r) => r.phase !== "failed");
  const now = useNow(processing ? SECOND : NOW_TICK_MS);

  // Announce status changes once, through the single polite region (design 7).
  const seen = useRef<Map<string, DocumentEntry["status"]> | null>(null);
  useEffect(() => {
    if (docs.version === 0) {
      return;
    }
    const before = seen.current;
    const after = new Map(docs.entries.map((e) => [e.id, e.status]));
    if (before) {
      for (const e of docs.entries) {
        const was = before.get(e.id);
        if (was === e.status) {
          continue;
        }
        if (e.status === "INDEXED") {
          announce(`${e.filename} is indexed.`);
        } else if (e.status === "FAILED") {
          announce(`${e.filename} failed: ${reasonText(e.failureReason)}`);
        }
      }
    }
    seen.current = after;
  }, [docs.version, docs.entries, announce]);

  const readiness: DocsReadiness = useMemo(() => {
    if (docs.load !== "ready") {
      return { kind: docs.load };
    }
    if (docs.entries.length === 0 && uploads.rows.length === 0) {
      return { kind: "none" };
    }
    if (!docs.entries.some((e) => e.status === "INDEXED")) {
      const count =
        docs.entries.filter((e) => e.status === "PROCESSING").length + uploads.rows.filter((r) => r.phase !== "failed").length;
      return { kind: "noneIndexed", processing: count };
    }
    return { kind: "ready" };
  }, [docs.load, docs.entries, uploads.rows]);

  const chips = useMemo(() => suggestions(docs.entries), [docs.entries]);
  const selectedIds = useMemo(() => [...selected], [selected]);

  return (
    <div className={styles.app}>
      <header className={styles.header}>
        <div>
          <h1 className={styles.title}>Pucho</h1>
          <p className={styles.subtitle}>Ask questions across your uploaded files. Answers cite their sources.</p>
        </div>
        <p className={styles.note}>
          <InfoIcon />
          Demo build · use public documents only
        </p>
      </header>
      <main className={styles.main}>
        <DocumentsPanel
          entries={docs.entries}
          localRows={uploads.rows}
          load={docs.load}
          poll={docs.poll}
          truncated={docs.truncated}
          now={now}
          selected={selected}
          onSelectionChange={(next) => {
            setCapError(undefined);
            setSelected(next);
          }}
          onCapExceeded={() => setCapError(`You can select up to ${MAX_SELECTED} documents.`)}
          onRefresh={docs.refresh}
          uploadError={uploads.error}
          onFiles={uploads.add}
          onRetryUpload={uploads.retry}
          onDismissUpload={uploads.dismiss}
          flash={flash}
        />
        <AskPanel
          docs={readiness}
          selectedIds={selectedIds}
          onClearSelection={() => {
            setCapError(undefined);
            setSelected(new Set());
          }}
          capError={capError}
          suggestions={chips}
          onRefreshDocuments={docs.refresh}
          stream={stream}
        />
      </main>
    </div>
  );
}

export default function App({ stream }: { stream?: AskStreamFn }) {
  return (
    <LiveRegionProvider>
      <ToastProvider>
        <Page stream={stream} />
      </ToastProvider>
    </LiveRegionProvider>
  );
}
