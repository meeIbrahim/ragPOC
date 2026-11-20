import { expect, test } from "vitest";
import { reasonText } from "./reasons";

// Copy is design 3.4 verbatim (amended 2026-11-19).
test.each([
  ["UNSUPPORTED_TYPE", "This file type or size isn't supported. Use PDF, TXT or MD up to 10 MB."],
  ["EMPTY_TEXT", "No text found in this file. Scanned PDFs need OCR, which isn't supported."],
  ["CORRUPT_FILE", "The file couldn't be read. It may be damaged."],
  ["DEPENDENCY_UNAVAILABLE", "Indexing services were unavailable on every attempt."],
  ["JOB_TIMEOUT", "Indexing took longer than 15 minutes. Try a smaller file."],
  ["POISON_MESSAGE", "Indexing kept crashing on this file."],
  ["INTERNAL", "Indexing failed with an internal error."],
  ["UPLOAD_FAILED", "The upload couldn't be processed. Try uploading again."],
])("%s reads as the design copy", (code, copy) => {
  expect(reasonText(code)).toBe(copy);
});

test.each([[undefined], [""], ["SOMETHING_NEW"], ["toString"]])("%s gives the fallback text", (code) => {
  expect(reasonText(code)).toBe("Indexing failed.");
});
