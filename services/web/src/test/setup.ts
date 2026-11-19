import "@testing-library/jest-dom/vitest";
import { cleanup } from "@testing-library/react";
import { afterEach, vi } from "vitest";

// Testing Library's async wrapper awaits a setTimeout(0) and only advances fake timers when it
// finds a `jest` global. Without this shim, user-event hangs under vi.useFakeTimers().
(globalThis as unknown as { jest: unknown }).jest = {
  advanceTimersByTime: (ms: number) => vi.advanceTimersByTime(ms),
};

// jsdom has no layout, so scrollIntoView is missing.
if (!Element.prototype.scrollIntoView) {
  Element.prototype.scrollIntoView = function scrollIntoView() {};
}

afterEach(() => {
  cleanup();
});
