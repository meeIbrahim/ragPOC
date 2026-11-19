import { useCallback, useEffect, useRef, useState } from "react";
import { ApiError, confirmUpload, createUpload } from "../api/http";
import type { DocumentEntry } from "../api/types";
import { putFile } from "../api/upload";
import { ALLOWED_EXTENSIONS, MAX_FILE_BYTES, NOT_LISTED_AFTER_POLLS } from "../limits";
import { formatMB } from "../lib/time";

/**
 * A row the server doesn't list yet. queued and uploading are before confirm; processing is
 * confirmed but not matched to a list entry; failed is a failure before confirm (nothing reached
 * the server, design 3.3 step 5).
 */
export type LocalRow =
  | { key: number; filename: string; phase: "queued"; ahead: number }
  | { key: number; filename: string; phase: "uploading"; progress: number }
  | { key: number; filename: string; phase: "processing"; uploadId: string; notListed: boolean }
  | { key: number; filename: string; phase: "failed" };

export interface Merge {
  /** The name the user just uploaded. */
  filename: string;
  /** The older entry that already holds this content. */
  entry: DocumentEntry;
}

interface Item {
  key: number;
  file: File;
  filename: string;
  phase: "queued" | "uploading" | "processing" | "watching" | "failed";
  progress: number;
  uploadId?: string;
  polls: number;
}

interface Options {
  entries: DocumentEntry[];
  /** Bumps once per successful GET /documents. */
  version: number;
  onConfirmed: () => void;
  onMerged: (m: Merge) => void;
}

function extension(name: string): string {
  const dot = name.lastIndexOf(".");
  return dot > 0 ? name.slice(dot).toLowerCase() : "";
}

function unsupported(name: string): string {
  return `"${name}" isn't supported. Use PDF, TXT or MD.`;
}

function check(file: File): string | undefined {
  if (!ALLOWED_EXTENSIONS.includes(extension(file.name))) {
    return unsupported(file.name);
  }
  if (file.size > MAX_FILE_BYTES) {
    return `"${file.name}" is ${formatMB(file.size)}. The limit is ${formatMB(MAX_FILE_BYTES)}.`;
  }
  return undefined;
}

/**
 * Uploads run one at a time in pick order (design 3.1). After confirm, each upload id is matched
 * against the server list: its own pending entry stands in for the local row, and the upload stays
 * watched until that entry leaves PROCESSING, because a duplicate usually shows only after hashing
 * folds the pending entry into an older one (design 3.3 as amended).
 */
export function useUploads({ entries, version, onConfirmed, onMerged }: Options) {
  const [items, setItems] = useState<Item[]>([]);
  const [error, setError] = useState<string | undefined>(undefined);
  const nextKey = useRef(0);
  const running = useRef<number | null>(null);
  const aborts = useRef(new Map<number, AbortController>());
  const itemsRef = useRef(items);
  itemsRef.current = items;
  const callbacks = useRef({ onConfirmed, onMerged });
  callbacks.current = { onConfirmed, onMerged };

  const update = useCallback((key: number, patch: Partial<Item>) => {
    setItems((list) => list.map((i) => (i.key === key ? { ...i, ...patch } : i)));
  }, []);

  const run = useCallback(
    async (item: Item) => {
      const ctrl = new AbortController();
      aborts.current.set(item.key, ctrl);
      const done = () => {
        aborts.current.delete(item.key);
        running.current = null;
      };
      try {
        const upload = await createUpload(item.filename);
        if (ctrl.signal.aborted) {
          return;
        }
        await putFile(upload.uploadUrl, item.file, (f) => update(item.key, { progress: f }), ctrl.signal);
        await confirmUpload(upload.id);
        if (ctrl.signal.aborted) {
          return;
        }
        done();
        update(item.key, { phase: "processing", uploadId: upload.id, polls: 0 });
        callbacks.current.onConfirmed();
      } catch (e) {
        if (ctrl.signal.aborted) {
          return;
        }
        done();
        if (e instanceof ApiError && e.kind === "badRequest") {
          // The server refused the name: like a client rejection, no row (design 3.1).
          setItems((list) => list.filter((i) => i.key !== item.key));
          setError(e.message.startsWith("unsupported file extension") ? unsupported(item.filename) : e.message);
          return;
        }
        update(item.key, { phase: "failed" });
      }
    },
    [update],
  );

  // Start the oldest queued upload when none is running.
  useEffect(() => {
    if (running.current !== null) {
      return;
    }
    const next = items.filter((i) => i.phase === "queued").sort((a, b) => a.key - b.key)[0];
    if (!next) {
      return;
    }
    running.current = next.key;
    update(next.key, { phase: "uploading", progress: 0 });
    void run(next);
  }, [items, run, update]);

  // Match confirmed uploads against each new server list.
  useEffect(() => {
    if (version === 0) {
      return;
    }
    const merges: Merge[] = [];
    const next: Item[] = [];
    for (const item of itemsRef.current) {
      if ((item.phase !== "processing" && item.phase !== "watching") || !item.uploadId) {
        next.push(item);
        continue;
      }
      const id = item.uploadId;
      const match = entries.find((e) => e.uploadIds.includes(id));
      if (!match) {
        next.push(item.phase === "processing" ? { ...item, polls: item.polls + 1 } : item);
      } else if (match.id !== id) {
        merges.push({ filename: item.filename, entry: match });
      } else if (match.status === "PROCESSING") {
        next.push({ ...item, phase: "watching" });
      }
      // Its own entry is INDEXED or FAILED: the server row is the whole story now.
    }
    setItems(next);
    merges.forEach((m) => callbacks.current.onMerged(m));
    // entries changes together with version; version alone marks a new poll.
  }, [version]);

  useEffect(() => {
    const map = aborts.current;
    return () => {
      map.forEach((c) => c.abort());
      map.clear();
    };
  }, []);

  const add = useCallback((files: File[]) => {
    let firstError: string | undefined;
    const accepted: Item[] = [];
    for (const file of files) {
      const problem = check(file);
      if (problem) {
        firstError ??= problem;
        continue;
      }
      nextKey.current += 1;
      accepted.push({ key: nextKey.current, file, filename: file.name, phase: "queued", progress: 0, polls: 0 });
    }
    setError(firstError);
    if (accepted.length > 0) {
      setItems((list) => [...list, ...accepted]);
    }
  }, []);

  const retry = useCallback((key: number) => {
    nextKey.current += 1;
    const newKey = nextKey.current;
    // A new key puts it at the back of the queue, in pick order.
    setItems((list) => list.map((i) => (i.key === key && i.phase === "failed" ? { ...i, key: newKey, phase: "queued", progress: 0 } : i)));
  }, []);

  const dismiss = useCallback((key: number) => {
    setItems((list) => list.filter((i) => !(i.key === key && i.phase === "failed")));
  }, []);

  const visible = items.filter((i) => i.phase !== "watching").sort((a, b) => b.key - a.key);
  const waiting = items.filter((i) => i.phase === "queued" || i.phase === "uploading").sort((a, b) => a.key - b.key);
  const rows: LocalRow[] = visible.map((i): LocalRow => {
    switch (i.phase) {
      case "queued":
        return { key: i.key, filename: i.filename, phase: "queued", ahead: waiting.findIndex((w) => w.key === i.key) };
      case "uploading":
        return { key: i.key, filename: i.filename, phase: "uploading", progress: i.progress };
      case "processing":
        return {
          key: i.key,
          filename: i.filename,
          phase: "processing",
          uploadId: i.uploadId ?? "",
          notListed: i.polls >= NOT_LISTED_AFTER_POLLS,
        };
      default:
        return { key: i.key, filename: i.filename, phase: "failed" };
    }
  });

  return {
    rows,
    error,
    /** Anything uploading, queued or confirmed but not listed: keep polling. */
    active: items.some((i) => i.phase !== "failed" && i.phase !== "watching"),
    add,
    retry,
    dismiss,
  };
}
