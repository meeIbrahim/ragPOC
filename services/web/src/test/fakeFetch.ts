import { vi } from "vitest";

export interface FetchCall {
  url: string;
  method: string;
  body: unknown;
  signal: AbortSignal | undefined;
}

type Handler = (call: FetchCall) => Response | Promise<Response>;

/** Replaces global fetch. Each call goes to the handler; calls are recorded in order. */
export function fakeFetch(handler: Handler) {
  const calls: FetchCall[] = [];
  const fn = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    const raw = init?.body;
    const call: FetchCall = {
      url,
      method: init?.method ?? "GET",
      body: typeof raw === "string" ? JSON.parse(raw) : raw,
      signal: init?.signal ?? undefined,
    };
    calls.push(call);
    if (call.signal?.aborted) {
      throw new DOMException("The operation was aborted.", "AbortError");
    }
    return handler(call);
  });
  vi.stubGlobal("fetch", fn);
  return { calls, fn };
}

export function json(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
}

/** A plain-text error the way Go's http.Error writes it: the message plus a newline. */
export function text(status: number, message: string): Response {
  return new Response(`${message}\n`, { status, headers: { "Content-Type": "text/plain; charset=utf-8" } });
}

export function networkError(): never {
  throw new TypeError("Failed to fetch");
}
