import type { WireDocument } from "../api/types";

export const T0 = Date.parse("2026-11-19T10:00:00Z");

export function wireDoc(over: Partial<WireDocument> & { id: string }): WireDocument {
  return {
    upload_ids: [over.id],
    filename: `${over.id}.md`,
    status: "INDEXED",
    created_at: new Date(T0).toISOString(),
    ...over,
  };
}

export function listBody(documents: WireDocument[], truncated = false) {
  return { documents, truncated };
}

export function setVisibility(state: "visible" | "hidden") {
  Object.defineProperty(document, "visibilityState", { configurable: true, get: () => state });
  document.dispatchEvent(new Event("visibilitychange"));
}
