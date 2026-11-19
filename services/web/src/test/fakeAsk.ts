import { act } from "@testing-library/react";
import { vi } from "vitest";
import type { AskEvent } from "../api/ask";
import type { AskStreamFn } from "../hooks/useAsk";

/** A stream the test feeds event by event; it ends as stopped when the signal aborts. */
export function controlled() {
  const queue: AskEvent[] = [];
  let wake: (() => void) | undefined;
  const calls: Array<{ question: string; ids: string[] }> = [];
  const fn: AskStreamFn = async function* (question, ids, signal) {
    calls.push({ question, ids });
    const aborted = new Promise<void>((r) => signal.addEventListener("abort", () => r(), { once: true }));
    for (;;) {
      while (queue.length > 0) {
        const e = queue.shift() as AskEvent;
        yield e;
        if (["done", "error", "invalid", "failed", "connectionLost", "stopped"].includes(e.type)) {
          return;
        }
      }
      const more = new Promise<void>((r) => (wake = r));
      await Promise.race([more, aborted]);
      if (signal.aborted) {
        yield { type: "stopped" };
        return;
      }
    }
  };
  const push = async (...events: AskEvent[]) => {
    queue.push(...events);
    await act(async () => {
      wake?.();
      await vi.advanceTimersByTimeAsync(0);
    });
  };
  return { fn, push, calls };
}

