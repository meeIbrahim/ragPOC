import { describe, expect, test } from "vitest";
import { formatElapsed, formatMB, formatRelative, formatSeconds, isSlow } from "./time";

const s = 1000;
const min = 60 * s;

describe("formatElapsed (processing rows, design 3.2)", () => {
  test.each([
    [0, "0 s"],
    [40 * s, "40 s"],
    [59 * s + 999, "59 s"],
    [60 * s, "1 min"],
    [3 * min + 59 * s, "3 min"],
  ])("%i ms reads %s", (ms, text) => {
    expect(formatElapsed(ms)).toBe(text);
  });

  test("a browser clock behind the server never shows a negative time (T6)", () => {
    expect(formatElapsed(-90 * s)).toBe("0 s");
  });
});

describe("isSlow", () => {
  test("is true from 5 minutes", () => {
    expect(isSlow(5 * min - 1)).toBe(false);
    expect(isSlow(5 * min)).toBe(true);
  });
});

describe("formatRelative (indexed rows)", () => {
  test.each([
    [-5 * s, "just now"],
    [59 * s, "just now"],
    [60 * s, "1 min ago"],
    [2 * min, "2 min ago"],
    [59 * min, "59 min ago"],
    [60 * min, "1 h ago"],
    [25 * 60 * min, "1 d ago"],
  ])("%i ms ago reads %s", (ms, text) => {
    expect(formatRelative(ms)).toBe(text);
  });
});

describe("formatSeconds (the Done line)", () => {
  test("one decimal, never negative", () => {
    expect(formatSeconds(6240)).toBe("6.2 s");
    expect(formatSeconds(-10)).toBe("0.0 s");
  });
});

describe("formatMB (size error, T1)", () => {
  const MB = 1_048_576;
  test("rounds up to whole MB, at least 1", () => {
    expect(formatMB(40 * MB)).toBe("40 MB");
    expect(formatMB(10 * MB + 1)).toBe("11 MB");
    expect(formatMB(1)).toBe("1 MB");
    expect(formatMB(0)).toBe("1 MB");
  });
});
