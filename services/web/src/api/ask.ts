import { bodyText } from "./http";
import { SseParser } from "./sse";
import { fromSources, type Source, type WireSource } from "./types";

/**
 * Everything POST /ask can produce, as events, so callers never need try/catch. The stream ends
 * after the first terminal event: done, error, invalid, failed, connectionLost or stopped.
 */
export type AskEvent =
  | { type: "sources"; sources: Source[] }
  | { type: "delta"; text: string }
  | { type: "done" }
  | { type: "error"; code: string; message: string }
  /** A 400 before the stream; message is the server's plain text. */
  | { type: "invalid"; message: string }
  /** Any other non-200 before the stream. */
  | { type: "failed"; status: number; message: string }
  /** The fetch rejected, or the stream ended with no done and no error. */
  | { type: "connectionLost" }
  | { type: "stopped" };

function isAbort(e: unknown, signal: AbortSignal): boolean {
  return signal.aborted || (e instanceof DOMException && e.name === "AbortError");
}

function parse(event: string, data: string): AskEvent | undefined {
  try {
    const body = JSON.parse(data) as Record<string, unknown>;
    switch (event) {
      case "sources":
        return { type: "sources", sources: fromSources(body as { sources: WireSource[] }) };
      case "delta":
        return { type: "delta", text: typeof body.text === "string" ? body.text : "" };
      case "done":
        return { type: "done" };
      case "error":
        return { type: "error", code: String(body.code ?? ""), message: String(body.message ?? "") };
      default:
        return undefined;
    }
  } catch {
    return undefined;
  }
}

const terminal = new Set<AskEvent["type"]>(["done", "error"]);

/** POST /ask over SSE with fetch + ReadableStream (spec A7; EventSource can't POST). */
export async function* askStream(question: string, uploadIds: string[], signal: AbortSignal): AsyncGenerator<AskEvent> {
  let res: Response;
  try {
    res = await fetch("/ask", {
      method: "POST",
      headers: { "Content-Type": "application/json", Accept: "text/event-stream" },
      body: JSON.stringify({ question, upload_ids: uploadIds }),
      signal,
    });
  } catch (e) {
    yield isAbort(e, signal) ? { type: "stopped" } : { type: "connectionLost" };
    return;
  }
  if (res.status === 400) {
    yield { type: "invalid", message: await bodyText(res) };
    return;
  }
  if (res.status !== 200 || !res.body) {
    yield { type: "failed", status: res.status, message: await bodyText(res) };
    return;
  }

  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  const parser = new SseParser();
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) {
        break;
      }
      for (const raw of parser.push(decoder.decode(value, { stream: true }))) {
        const e = parse(raw.event, raw.data);
        if (!e) {
          continue;
        }
        yield e;
        if (terminal.has(e.type)) {
          return;
        }
      }
      if (signal.aborted) {
        yield { type: "stopped" };
        return;
      }
    }
  } catch (e) {
    yield isAbort(e, signal) ? { type: "stopped" } : { type: "connectionLost" };
    return;
  } finally {
    reader.releaseLock();
    if (signal.aborted) {
      res.body.cancel().catch(() => {});
    }
  }
  yield signal.aborted ? { type: "stopped" } : { type: "connectionLost" };
}
