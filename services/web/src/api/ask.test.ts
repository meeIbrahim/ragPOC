import { afterEach, describe, expect, test, vi } from "vitest";
import { fakeFetch, networkError, text } from "../test/fakeFetch";
import { event, sseResponse } from "../test/sseStream";
import { askStream, type AskEvent } from "./ask";

afterEach(() => {
  vi.unstubAllGlobals();
});

async function collect(question = "q", ids: string[] = [], signal = new AbortController().signal): Promise<AskEvent[]> {
  const out: AskEvent[] = [];
  for await (const e of askStream(question, ids, signal)) {
    out.push(e);
  }
  return out;
}

describe("askStream", () => {
  test("posts the question and upload ids, then yields sources, deltas and done in order", async () => {
    const { calls } = fakeFetch(() =>
      sseResponse([
        event("sources", { sources: [{ filename: "a.md", snippet: "s", score: 0.7 }] }),
        event("delta", { text: "Hello " }),
        event("delta", { text: "[1]" }),
        event("done", {}),
      ]),
    );
    expect(await collect("why?", ["u1"])).toEqual([
      { type: "sources", sources: [{ filename: "a.md", snippet: "s", score: 0.7 }] },
      { type: "delta", text: "Hello " },
      { type: "delta", text: "[1]" },
      { type: "done" },
    ]);
    expect(calls[0]).toMatchObject({ url: "/ask", method: "POST", body: { question: "why?", upload_ids: ["u1"] } });
  });

  test("an error event carries its code and message and ends the stream", async () => {
    fakeFetch(() =>
      sseResponse([event("sources", { sources: [] }), event("error", { code: "UNAVAILABLE", message: "down" }), event("delta", { text: "x" })]),
    );
    expect(await collect()).toEqual([
      { type: "sources", sources: [] },
      { type: "error", code: "UNAVAILABLE", message: "down" },
    ]);
  });

  test("an unknown event name is ignored", async () => {
    fakeFetch(() => sseResponse([event("ping", {}), event("done", {})]));
    expect(await collect()).toEqual([{ type: "done" }]);
  });

  test("a 400 before the stream keeps the server's text", async () => {
    fakeFetch(() => text(400, "at most 100 upload_ids may be selected"));
    expect(await collect()).toEqual([{ type: "invalid", message: "at most 100 upload_ids may be selected" }]);
  });

  test("a 5xx before the stream is a failure with the server's text", async () => {
    fakeFetch(() => text(500, "internal error"));
    expect(await collect()).toEqual([{ type: "failed", status: 500, message: "internal error" }]);
  });

  test("a stream that ends with no done and no error is a lost connection", async () => {
    fakeFetch(() => sseResponse([event("delta", { text: "partial" })]));
    expect(await collect()).toEqual([{ type: "delta", text: "partial" }, { type: "connectionLost" }]);
  });

  test("a stream that errors mid-way, or a fetch that rejects, is a lost connection", async () => {
    fakeFetch(() => sseResponse([event("delta", { text: "x" })], { end: "error" }));
    expect(await collect()).toEqual([{ type: "delta", text: "x" }, { type: "connectionLost" }]);
    fakeFetch(() => networkError());
    expect(await collect()).toEqual([{ type: "connectionLost" }]);
  });

  test("abort ends the stream as stopped, not an error", async () => {
    const ctrl = new AbortController();
    fakeFetch((call) => sseResponse([event("sources", { sources: [] })], { end: "hang", signal: call.signal }));
    const out: AskEvent[] = [];
    for await (const e of askStream("q", [], ctrl.signal)) {
      out.push(e);
      ctrl.abort();
    }
    expect(out).toEqual([{ type: "sources", sources: [] }, { type: "stopped" }]);
  });

  test("abort before the response arrives is stopped too", async () => {
    const ctrl = new AbortController();
    ctrl.abort();
    fakeFetch(() => sseResponse([]));
    expect(await collect("q", [], ctrl.signal)).toEqual([{ type: "stopped" }]);
  });
});
