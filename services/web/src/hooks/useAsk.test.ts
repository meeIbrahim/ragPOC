import { act, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import { controlled } from "../test/fakeAsk";
import { useAsk, validateQuestion, type AskStreamFn } from "./useAsk";

const src = (n: number) => ({ filename: `f${n}.md`, snippet: `s${n}`, score: 0.5 });

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(1_000_000);
});
afterEach(() => {
  vi.useRealTimers();
});

describe("validateQuestion (spec plan review amendments)", () => {
  test("whitespace-only is empty", () => {
    expect(validateQuestion("  \n\t ")).toBe("Question is empty.");
  });

  test("length counts code points: 2000 emoji pass, 2001 fail", () => {
    expect(validateQuestion("😀".repeat(2000))).toBeUndefined();
    expect(validateQuestion("😀".repeat(2001))).toBe("Question is longer than 2000 characters.");
  });
});

describe("useAsk", () => {
  test("sending, sources, streaming, then done with source count and elapsed time", async () => {
    const s = controlled();
    const { result } = renderHook(() => useAsk(s.fn));
    await act(async () => result.current.ask("why?", ["u1"]));
    expect(result.current.state).toMatchObject({ phase: "sending", question: "why?" });
    expect(s.calls[0]).toEqual({ question: "why?", ids: ["u1"] });
    await s.push({ type: "sources", sources: [src(1), src(2)] });
    expect(result.current.state).toMatchObject({ phase: "sources", sources: [src(1), src(2)] });
    await s.push({ type: "delta", text: "Hel" }, { type: "delta", text: "lo [1]" });
    expect(result.current.state).toMatchObject({ phase: "streaming", answer: "Hello [1]" });
    expect(result.current.busy).toBe(true);
    vi.setSystemTime(1_006_200);
    await s.push({ type: "done" });
    expect(result.current.state).toMatchObject({ phase: "done", answer: "Hello [1]", sources: [src(1), src(2)], elapsedMs: 6200 });
    expect(result.current.busy).toBe(false);
  });

  test("sources: [] is nothing relevant; later deltas are ignored", async () => {
    const s = controlled();
    const { result } = renderHook(() => useAsk(s.fn));
    await act(async () => result.current.ask("q", ["u1"]));
    await s.push({ type: "sources", sources: [] }, { type: "delta", text: "No relevant information…" }, { type: "done" });
    expect(result.current.state).toMatchObject({ phase: "nothingRelevant", question: "q", hadSelection: true });
  });

  test("stop() keeps the partial answer and the question", async () => {
    const s = controlled();
    const { result } = renderHook(() => useAsk(s.fn));
    await act(async () => result.current.ask("q", []));
    await s.push({ type: "sources", sources: [src(1)] }, { type: "delta", text: "part" });
    await act(async () => {
      result.current.stop();
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(result.current.state).toMatchObject({ phase: "stopped", question: "q", answer: "part", sources: [src(1)] });
  });

  test("stop() before sources gives stopped with no sources (edge case 3)", async () => {
    const s = controlled();
    const { result } = renderHook(() => useAsk(s.fn));
    await act(async () => result.current.ask("q", []));
    await act(async () => {
      result.current.stop();
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(result.current.state).toMatchObject({ phase: "stopped", question: "q", sources: [], answer: "" });
  });

  test("UNAVAILABLE is the unavailable kind; partial text stays", async () => {
    const s = controlled();
    const { result } = renderHook(() => useAsk(s.fn));
    await act(async () => result.current.ask("q", []));
    await s.push({ type: "sources", sources: [src(1)] }, { type: "delta", text: "half" }, { type: "error", code: "UNAVAILABLE", message: "down" });
    expect(result.current.state).toMatchObject({ phase: "error", kind: "unavailable", answer: "half", question: "q" });
  });

  test("other codes and pre-stream 5xx are the other kind with the message", async () => {
    const s = controlled();
    const { result } = renderHook(() => useAsk(s.fn));
    await act(async () => result.current.ask("q", []));
    await s.push({ type: "error", code: "INTERNAL", message: "boom" });
    expect(result.current.state).toMatchObject({ phase: "error", kind: "other", message: "boom" });
    await act(async () => result.current.ask("q", []));
    await s.push({ type: "failed", status: 502, message: "bad gateway" });
    expect(result.current.state).toMatchObject({ phase: "error", kind: "other", message: "bad gateway" });
  });

  test.each([
    ["at most 100 upload_ids may be selected", "selection"],
    ['unknown upload_id: "x"', "selection"],
    ["question is empty", "question"],
  ])("a 400 %j targets the %s", async (message, target) => {
    const s = controlled();
    const { result } = renderHook(() => useAsk(s.fn));
    await act(async () => result.current.ask("q", []));
    await s.push({ type: "invalid", message });
    expect(result.current.state).toMatchObject({ phase: "invalid", message, target, question: "q" });
  });

  test("an early end is connection lost", async () => {
    const s = controlled();
    const { result } = renderHook(() => useAsk(s.fn));
    await act(async () => result.current.ask("q", []));
    await s.push({ type: "delta", text: "x" }, { type: "connectionLost" });
    expect(result.current.state).toMatchObject({ phase: "error", kind: "connectionLost", answer: "x" });
  });

  test("a stream that throws ends as a lost connection, not an unhandled rejection", async () => {
    const throwing: AskStreamFn = async function* () {
      yield { type: "delta", text: "x" };
      throw new Error("boom");
    };
    const { result } = renderHook(() => useAsk(throwing));
    await act(async () => {
      result.current.ask("q", []);
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(result.current.state).toMatchObject({ phase: "error", kind: "connectionLost", answer: "x" });
  });

  test("retry() resends the same question and scope", async () => {
    const s = controlled();
    const { result } = renderHook(() => useAsk(s.fn));
    await act(async () => result.current.ask("why?", ["u1", "u2"]));
    await s.push({ type: "error", code: "UNAVAILABLE", message: "down" });
    await act(async () => result.current.retry());
    expect(s.calls).toEqual([
      { question: "why?", ids: ["u1", "u2"] },
      { question: "why?", ids: ["u1", "u2"] },
    ]);
    expect(result.current.state.phase).toBe("sending");
  });

  test("unmount aborts the stream with no state update after it", async () => {
    const s = controlled();
    let signal: AbortSignal | undefined;
    const spy: AskStreamFn = (q, ids, sig) => {
      signal = sig;
      return s.fn(q, ids, sig);
    };
    const errors = vi.spyOn(console, "error").mockImplementation(() => {});
    const { result, unmount } = renderHook(() => useAsk(spy));
    await act(async () => result.current.ask("q", []));
    unmount();
    expect(signal?.aborted).toBe(true);
    await s.push({ type: "delta", text: "late" });
    expect(errors).not.toHaveBeenCalled();
    errors.mockRestore();
  });
});
