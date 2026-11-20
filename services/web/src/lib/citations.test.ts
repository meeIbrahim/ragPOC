import { expect, test } from "vitest";
import { splitCitations } from "./citations";

test("text and in-range [n] markers become segments", () => {
  expect(splitCitations("Owners [1] and scope [2].", 2)).toEqual([
    { type: "text", text: "Owners " },
    { type: "cite", n: 1 },
    { type: "text", text: " and scope " },
    { type: "cite", n: 2 },
    { type: "text", text: "." },
  ]);
});

test("a number above the source count stays text (design 5.3)", () => {
  expect(splitCitations("see [4] and [0]", 3)).toEqual([{ type: "text", text: "see [4] and [0]" }]);
});

test("two-digit and adjacent markers", () => {
  expect(splitCitations("[12][1][2]", 12)).toEqual([
    { type: "cite", n: 12 },
    { type: "cite", n: 1 },
    { type: "cite", n: 2 },
  ]);
});

test("a marker split across deltas parses the same once accumulated (T4)", () => {
  let acc = "";
  for (const delta of ["see [1", "2] and [3]"]) {
    acc += delta;
  }
  expect(splitCitations(acc, 12)).toEqual(splitCitations("see [12] and [3]", 12));
  expect(splitCitations(acc, 12)).toContainEqual({ type: "cite", n: 12 });
  // Mid-stream the half marker is plain text, never a wrong pill.
  expect(splitCitations("see [1", 12)).toEqual([{ type: "text", text: "see [1" }]);
});

test("no sources means no pills", () => {
  expect(splitCitations("x [1]", 0)).toEqual([{ type: "text", text: "x [1]" }]);
});
