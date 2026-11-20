import { describe, expect, test } from "vitest";
import { SseParser, type SseEvent } from "./sse";

function parseAll(chunks: string[]): { events: SseEvent[]; incomplete: boolean } {
  const p = new SseParser();
  const events = chunks.flatMap((c) => p.push(c));
  return { events, incomplete: p.end() };
}

const two = 'event: sources\ndata: {"sources":[]}\n\nevent: delta\ndata: {"text":"héllo 👋"}\n\n';
const twoEvents: SseEvent[] = [
  { event: "sources", data: '{"sources":[]}' },
  { event: "delta", data: '{"text":"héllo 👋"}' },
];

describe("SseParser", () => {
  test("one event", () => {
    expect(parseAll(['event: done\ndata: {}\n\n'])).toEqual({ events: [{ event: "done", data: "{}" }], incomplete: false });
  });

  test("several events in one chunk", () => {
    expect(parseAll([two]).events).toEqual(twoEvents);
  });

  test("a split at every byte offset parses the same as one read (spec edge case 1)", () => {
    const bytes = new TextEncoder().encode(two);
    for (let i = 0; i <= bytes.length; i++) {
      const decoder = new TextDecoder();
      const chunks = [decoder.decode(bytes.slice(0, i), { stream: true }), decoder.decode(bytes.slice(i))];
      expect(parseAll(chunks), `split at byte ${i}`).toEqual({ events: twoEvents, incomplete: false });
    }
  });

  test("multi-line data is joined with a newline", () => {
    expect(parseAll(["event: x\ndata: a\ndata: b\n\n"]).events).toEqual([{ event: "x", data: "a\nb" }]);
  });

  test("CRLF line endings, including a CR and LF split across chunks", () => {
    expect(parseAll(["event: done\r\ndata: {}\r", "\n\r\n"]).events).toEqual([{ event: "done", data: "{}" }]);
  });

  test("an event without a name is a message; comments are skipped", () => {
    expect(parseAll([": keep-alive\n\ndata: x\n\n"]).events).toEqual([{ event: "message", data: "x" }]);
  });

  test("a value without a space after the colon keeps all its text", () => {
    expect(parseAll(["event:done\ndata:{}\n\n"]).events).toEqual([{ event: "done", data: "{}" }]);
  });

  test("a trailing partial event is reported as incomplete, not dispatched", () => {
    expect(parseAll(['event: delta\ndata: {"text":"x"}\n'])).toEqual({ events: [], incomplete: true });
    expect(parseAll(["event: del"])).toEqual({ events: [], incomplete: true });
  });
});
