import { expect, test } from "vitest";
import type { DocumentEntry } from "../api/types";
import { readableName, suggestions } from "./suggestions";

function entry(filename: string, createdAt: number, status: DocumentEntry["status"] = "INDEXED"): DocumentEntry {
  return { id: filename, uploadIds: [filename], filename, status, failureReason: undefined, createdAt };
}

test("readable names drop the extension and turn - and _ into spaces", () => {
  expect(readableName("kubernetes-pods.md")).toBe("kubernetes pods");
  expect(readableName("rust__ownership_v2.PDF")).toBe("rust ownership v2");
  expect(readableName("README")).toBe("README");
});

test("chips come from the newest indexed files; compare needs two (design 5.2)", () => {
  const docs = [entry("old.md", 1), entry("kubernetes-pods.md", 3), entry("rust-ownership.md", 2), entry("busy.pdf", 4, "PROCESSING")];
  expect(suggestions(docs)).toEqual([
    "Summarize kubernetes pods",
    "Key points of rust ownership",
    "Compare kubernetes pods and rust ownership",
  ]);
});

test("one indexed file gives no compare chip; none gives no chips", () => {
  expect(suggestions([entry("notes.md", 1)])).toEqual(["Summarize notes", "Key points of notes"]);
  expect(suggestions([entry("x.md", 1, "FAILED")])).toEqual([]);
});
