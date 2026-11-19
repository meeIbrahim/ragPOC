import { fromDocumentList, fromUpload, type DocumentList, type Upload, type WireDocumentList, type WireUpload } from "./types";

/**
 * badRequest: a 400 with the server's plain text. notConfirmable: confirm answered 404, 409 or
 * 410 (the intent is gone, failed or expired). server: any other non-2xx. network: no response.
 * aborted: the caller cancelled.
 */
export type ApiErrorKind = "badRequest" | "notConfirmable" | "server" | "network" | "aborted";

export class ApiError extends Error {
  readonly kind: ApiErrorKind;
  readonly status: number | undefined;

  constructor(kind: ApiErrorKind, message: string, status?: number) {
    super(message);
    this.name = "ApiError";
    this.kind = kind;
    this.status = status;
  }
}

async function send(path: string, init: RequestInit): Promise<Response> {
  try {
    return await fetch(path, init);
  } catch (e) {
    if (e instanceof DOMException && e.name === "AbortError") {
      throw new ApiError("aborted", "request aborted");
    }
    throw new ApiError("network", "Couldn't reach the server.");
  }
}

/** Go's http.Error body is the message plus a newline. */
export async function bodyText(res: Response): Promise<string> {
  try {
    return (await res.text()).trim();
  } catch {
    return "";
  }
}

async function failure(res: Response): Promise<ApiError> {
  const message = (await bodyText(res)) || `HTTP ${res.status}`;
  return new ApiError(res.status === 400 ? "badRequest" : "server", message, res.status);
}

export async function createUpload(filename: string): Promise<Upload> {
  const res = await send("/upload", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ filename }),
  });
  if (!res.ok) {
    throw await failure(res);
  }
  return fromUpload((await res.json()) as WireUpload);
}

export async function confirmUpload(id: string): Promise<void> {
  const res = await send(`/upload/${encodeURIComponent(id)}/confirm`, { method: "POST" });
  if (res.status === 404 || res.status === 409 || res.status === 410) {
    throw new ApiError("notConfirmable", await bodyText(res), res.status);
  }
  if (!res.ok) {
    throw await failure(res);
  }
}

export async function listDocuments(signal?: AbortSignal): Promise<DocumentList> {
  const res = await send("/documents", signal ? { signal } : {});
  if (!res.ok) {
    throw await failure(res);
  }
  return fromDocumentList((await res.json()) as WireDocumentList);
}
