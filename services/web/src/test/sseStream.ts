/**
 * A text/event-stream Response whose body emits the given chunks. `end` decides how the body
 * finishes: "close" ends it, "error" errors the stream, "hang" never ends unless the signal aborts.
 */
export function sseResponse(
  chunks: Array<string | Uint8Array>,
  opts: { end?: "close" | "error" | "hang"; signal?: AbortSignal } = {},
): Response {
  const encoder = new TextEncoder();
  const end = opts.end ?? "close";
  const queue = chunks.map((c) => (typeof c === "string" ? encoder.encode(c) : c));
  // Pull-based, so an "error" end still delivers every chunk first (error() drops queued chunks).
  const body = new ReadableStream<Uint8Array>({
    start(controller) {
      if (end === "hang") {
        opts.signal?.addEventListener("abort", () => {
          controller.error(new DOMException("The operation was aborted.", "AbortError"));
        });
      }
    },
    pull(controller) {
      const next = queue.shift();
      if (next) {
        controller.enqueue(next);
      } else if (end === "close") {
        controller.close();
      } else if (end === "error") {
        controller.error(new TypeError("network error"));
      } else {
        return new Promise<void>(() => {});
      }
      return undefined;
    },
  });
  return new Response(body, { status: 200, headers: { "Content-Type": "text/event-stream" } });
}

export function event(name: string, data: unknown): string {
  return `event: ${name}\ndata: ${JSON.stringify(data)}\n\n`;
}
