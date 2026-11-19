// Wire types mirror the Go JSON (snake_case, PRD 4.6). Domain types are camelCase; each has one
// explicit from… mapper (spec A4). Mappers default a missing field; fromUpload throws only when
// the presigned URL is missing, because no upload can work without it.

export type EntryStatus = "PROCESSING" | "INDEXED" | "FAILED";

export interface WireDocument {
  id: string;
  upload_ids: string[];
  filename: string;
  status: string;
  failure_reason?: string;
  created_at: string;
}

export interface WireDocumentList {
  documents: WireDocument[];
  truncated: boolean;
}

export interface WireUpload {
  id: string;
  filename: string;
  upload_url: string;
  status?: string;
  created_at?: string;
  expires_at?: string;
}

export interface WireSource {
  filename: string;
  snippet: string;
  score: number;
}

export interface DocumentEntry {
  /** The oldest linked upload id; stable for the entry's life (feature 3 D1). */
  id: string;
  uploadIds: string[];
  filename: string;
  status: EntryStatus;
  /** Set only when status is FAILED. */
  failureReason: string | undefined;
  /** Epoch milliseconds (the API sends UTC). */
  createdAt: number;
}

export interface DocumentList {
  documents: DocumentEntry[];
  truncated: boolean;
}

export interface Upload {
  id: string;
  filename: string;
  uploadUrl: string;
}

export interface Source {
  filename: string;
  snippet: string;
  score: number;
}

function toStatus(s: string): EntryStatus {
  // The list never shows WAITING or EXPIRED; anything unexpected reads as still in progress.
  return s === "INDEXED" || s === "FAILED" ? s : "PROCESSING";
}

function parseTime(s: string | undefined): number {
  const t = s ? Date.parse(s) : Number.NaN;
  return Number.isFinite(t) ? t : Date.now();
}

export function fromDocument(w: WireDocument): DocumentEntry {
  return {
    id: w.id,
    uploadIds: w.upload_ids?.length ? w.upload_ids : [w.id],
    filename: w.filename ?? "",
    status: toStatus(w.status),
    failureReason: w.failure_reason || undefined,
    createdAt: parseTime(w.created_at),
  };
}

export function fromDocumentList(w: WireDocumentList): DocumentList {
  return {
    documents: (w?.documents ?? []).map(fromDocument),
    truncated: w?.truncated ?? false,
  };
}

export function fromUpload(w: WireUpload): Upload {
  if (!w?.upload_url) {
    throw new Error("POST /upload answered without upload_url");
  }
  return { id: w.id, filename: w.filename, uploadUrl: w.upload_url };
}

export function fromSources(w: { sources: WireSource[] }): Source[] {
  return (w?.sources ?? []).map((s) => ({
    filename: s.filename ?? "",
    snippet: s.snippet ?? "",
    score: Number.isFinite(s.score) ? s.score : 0,
  }));
}
