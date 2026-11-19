import { expect, test } from "vitest";
import { fromDocumentList, fromSources, fromUpload } from "./types";

test("a document list maps to camelCase entries; an absent failure_reason stays undefined", () => {
  const list = fromDocumentList({
    documents: [
      {
        id: "u1",
        upload_ids: ["u1", "u2"],
        filename: "notes.md",
        status: "INDEXED",
        created_at: "2026-11-19T10:00:00Z",
      },
      {
        id: "u3",
        upload_ids: ["u3"],
        filename: "scan.pdf",
        status: "FAILED",
        failure_reason: "EMPTY_TEXT",
        created_at: "2026-11-19T09:00:00Z",
      },
    ],
    truncated: true,
  });
  expect(list.truncated).toBe(true);
  expect(list.documents[0]).toEqual({
    id: "u1",
    uploadIds: ["u1", "u2"],
    filename: "notes.md",
    status: "INDEXED",
    failureReason: undefined,
    createdAt: Date.parse("2026-11-19T10:00:00Z"),
  });
  expect(list.documents[1]?.failureReason).toBe("EMPTY_TEXT");
});

test("a list response with missing fields is read as empty, never a crash", () => {
  expect(fromDocumentList({} as never)).toEqual({ documents: [], truncated: false });
  const [entry] = fromDocumentList({
    documents: [{ id: "u1", filename: "a.md", status: "PROCESSING", created_at: "2026-11-19T10:00:00Z" } as never],
    truncated: false,
  }).documents;
  expect(entry?.uploadIds).toEqual(["u1"]);
});

test("an upload response maps upload_url to uploadUrl", () => {
  expect(
    fromUpload({
      id: "u1",
      filename: "a.md",
      upload_url: "http://minio/x",
      status: "WAITING",
      created_at: "2026-11-19T10:00:00Z",
      expires_at: "2026-11-19T10:15:00Z",
    }),
  ).toEqual({ id: "u1", filename: "a.md", uploadUrl: "http://minio/x" });
});

test("a source without a numeric score reads 0, so rendering never throws", () => {
  expect(fromSources({ sources: [{ filename: "a.md", snippet: "s" } as never] })[0]?.score).toBe(0);
});

test("a missing or invalid created_at falls back to now, never NaN", () => {
  const [entry] = fromDocumentList({
    documents: [{ id: "u1", upload_ids: ["u1"], filename: "a.md", status: "INDEXED", created_at: "garbage" }],
    truncated: false,
  }).documents;
  expect(Number.isFinite(entry?.createdAt)).toBe(true);
});

test("an upload response without upload_url is rejected", () => {
  expect(() => fromUpload({ id: "u1", filename: "a.md" } as never)).toThrow(/upload_url/);
});

test("sources map one to one; a missing list is empty", () => {
  expect(fromSources({ sources: [{ filename: "a.md", snippet: "s", score: 0.71 }] })).toEqual([
    { filename: "a.md", snippet: "s", score: 0.71 },
  ]);
  expect(fromSources({} as never)).toEqual([]);
});
