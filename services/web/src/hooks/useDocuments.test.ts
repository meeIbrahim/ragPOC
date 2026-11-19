import { act, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import { POLL_INTERVAL_MS } from "../limits";
import { fakeFetch, json, networkError, text } from "../test/fakeFetch";
import { listBody, setVisibility, wireDoc } from "../test/fixtures";
import { useDocuments } from "./useDocuments";
import { useNow } from "./useNow";

async function tick(ms: number) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms);
  });
}

beforeEach(() => {
  vi.useFakeTimers();
  setVisibility("visible");
});
afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe("useDocuments", () => {
  test("loads once on mount and maps the entries", async () => {
    const { calls } = fakeFetch(() => json(200, listBody([wireDoc({ id: "a" })], true)));
    const { result } = renderHook(() => useDocuments(false));
    expect(result.current.load).toBe("loading");
    await tick(0);
    expect(result.current.load).toBe("ready");
    expect(result.current.entries.map((e) => e.id)).toEqual(["a"]);
    expect(result.current.truncated).toBe(true);
    await tick(POLL_INTERVAL_MS * 5);
    expect(calls, "no polling while nothing is active").toHaveLength(1);
  });

  test("polls every 2 s only while active, and stops when not", async () => {
    const { calls } = fakeFetch(() => json(200, listBody([])));
    const { rerender } = renderHook(({ active }) => useDocuments(active), { initialProps: { active: true } });
    await tick(0);
    expect(calls).toHaveLength(1);
    await tick(POLL_INTERVAL_MS);
    expect(calls).toHaveLength(2);
    await tick(POLL_INTERVAL_MS);
    expect(calls).toHaveLength(3);
    rerender({ active: false });
    await tick(POLL_INTERVAL_MS * 3);
    expect(calls).toHaveLength(3);
  });

  test("a PROCESSING entry keeps polling on its own until it settles", async () => {
    let status = "PROCESSING";
    const { calls } = fakeFetch(() => json(200, listBody([wireDoc({ id: "a", status })])));
    renderHook(() => useDocuments(false));
    await tick(0);
    await tick(POLL_INTERVAL_MS);
    expect(calls).toHaveLength(2);
    status = "INDEXED";
    await tick(POLL_INTERVAL_MS);
    expect(calls).toHaveLength(3);
    await tick(POLL_INTERVAL_MS * 3);
    expect(calls, "settled: polling stops").toHaveLength(3);
  });

  test("an initial failure is a failed load; refresh() retries at once", async () => {
    let fail = true;
    const { calls } = fakeFetch(() => (fail ? networkError() : json(200, listBody([wireDoc({ id: "a" })]))));
    const { result } = renderHook(() => useDocuments(false));
    await tick(0);
    expect(result.current.load).toBe("failed");
    fail = false;
    await act(async () => {
      result.current.refresh();
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(calls).toHaveLength(2);
    expect(result.current.load).toBe("ready");
  });

  test("one failure reconnects, three in a row are stale, a success clears it; entries are kept", async () => {
    let failing = false;
    fakeFetch(() => (failing ? text(503, "unavailable") : json(200, listBody([wireDoc({ id: "a" })]))));
    const { result } = renderHook(() => useDocuments(true));
    await tick(0);
    expect(result.current.poll).toBe("ok");
    failing = true;
    await tick(POLL_INTERVAL_MS);
    expect(result.current.poll).toBe("reconnecting");
    await tick(POLL_INTERVAL_MS);
    expect(result.current.poll).toBe("reconnecting");
    await tick(POLL_INTERVAL_MS);
    expect(result.current.poll).toBe("stale");
    expect(result.current.entries.map((e) => e.id), "a failed poll keeps the rows").toEqual(["a"]);
    expect(result.current.load).toBe("ready");
    failing = false;
    await tick(POLL_INTERVAL_MS);
    expect(result.current.poll).toBe("ok");
  });

  test("a hidden tab pauses polling; showing it fetches at once", async () => {
    const { calls } = fakeFetch(() => json(200, listBody([])));
    renderHook(() => useDocuments(true));
    await tick(0);
    await act(async () => {
      setVisibility("hidden");
    });
    await tick(POLL_INTERVAL_MS * 4);
    expect(calls).toHaveLength(1);
    await act(async () => {
      setVisibility("visible");
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(calls).toHaveLength(2);
    await tick(POLL_INTERVAL_MS);
    expect(calls).toHaveLength(3);
  });

  test("version counts successful fetches", async () => {
    fakeFetch(() => json(200, listBody([])));
    const { result } = renderHook(() => useDocuments(true));
    await tick(0);
    expect(result.current.version).toBe(1);
    await tick(POLL_INTERVAL_MS);
    expect(result.current.version).toBe(2);
  });

  test("no state update after unmount", async () => {
    const errors = vi.spyOn(console, "error").mockImplementation(() => {});
    fakeFetch(() => new Promise((r) => setTimeout(() => r(json(200, listBody([]))), 100)));
    const { unmount } = renderHook(() => useDocuments(true));
    unmount();
    await tick(1000);
    expect(errors).not.toHaveBeenCalled();
    errors.mockRestore();
  });
});

describe("useNow", () => {
  test("ticks at the given period", async () => {
    vi.setSystemTime(1_000_000);
    const { result } = renderHook(() => useNow(15_000));
    expect(result.current).toBe(1_000_000);
    await tick(14_999);
    expect(result.current).toBe(1_000_000);
    await tick(1);
    expect(result.current).toBe(1_015_000);
  });
});
