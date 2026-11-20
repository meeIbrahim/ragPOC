import { afterEach, describe, expect, test, vi } from "vitest";
import { fakeFetch, json, networkError, text } from "../test/fakeFetch";
import { ApiError, confirmUpload, createUpload, listDocuments } from "./http";

afterEach(() => {
  vi.unstubAllGlobals();
});

async function caught(p: Promise<unknown>): Promise<ApiError> {
  try {
    await p;
  } catch (e) {
    expect(e).toBeInstanceOf(ApiError);
    return e as ApiError;
  }
  throw new Error("expected the call to fail");
}

describe("createUpload", () => {
  test("201 posts the filename and returns the id and presigned URL", async () => {
    const { calls } = fakeFetch(() =>
      json(201, { id: "u1", filename: "a.md", upload_url: "http://minio/x", status: "WAITING" }),
    );
    await expect(createUpload("a.md")).resolves.toEqual({ id: "u1", filename: "a.md", uploadUrl: "http://minio/x" });
    expect(calls[0]).toMatchObject({ url: "/upload", method: "POST", body: { filename: "a.md" } });
  });

  test("a 400 keeps the server's plain text without the trailing newline", async () => {
    fakeFetch(() => text(400, "unsupported file extension: .png"));
    const e = await caught(createUpload("a.png"));
    expect(e).toMatchObject({ kind: "badRequest", status: 400, message: "unsupported file extension: .png" });
  });

  test("a 5xx is a server error", async () => {
    fakeFetch(() => text(500, "internal error"));
    expect(await caught(createUpload("a.md"))).toMatchObject({ kind: "server", status: 500 });
  });

  test("a network failure is distinct from an HTTP error", async () => {
    fakeFetch(() => networkError());
    expect(await caught(createUpload("a.md"))).toMatchObject({ kind: "network" });
  });
});

describe("confirmUpload", () => {
  test("202 confirms the upload by id", async () => {
    const { calls } = fakeFetch(() => json(202, { id: "u 1", status: "UPLOADED" }));
    await expect(confirmUpload("u 1")).resolves.toBeUndefined();
    expect(calls[0]).toMatchObject({ url: "/upload/u%201/confirm", method: "POST" });
  });

  test.each([404, 409, 410])("%i means the upload can't be confirmed", async (status) => {
    fakeFetch(() => text(status, "upload intent expired"));
    expect(await caught(confirmUpload("u1"))).toMatchObject({ kind: "notConfirmable", status });
  });

  test("a 5xx is a server error", async () => {
    fakeFetch(() => text(500, "internal error"));
    expect(await caught(confirmUpload("u1"))).toMatchObject({ kind: "server", status: 500 });
  });
});

describe("listDocuments", () => {
  test("200 returns mapped entries", async () => {
    fakeFetch(() =>
      json(200, {
        documents: [{ id: "u1", upload_ids: ["u1"], filename: "a.md", status: "INDEXED", created_at: "2026-11-19T10:00:00Z" }],
        truncated: false,
      }),
    );
    const list = await listDocuments();
    expect(list.documents.map((d) => d.filename)).toEqual(["a.md"]);
  });

  test("a 5xx and a network failure are errors", async () => {
    fakeFetch(() => text(503, "unavailable"));
    expect(await caught(listDocuments())).toMatchObject({ kind: "server", status: 503 });
    fakeFetch(() => networkError());
    expect(await caught(listDocuments())).toMatchObject({ kind: "network" });
  });
});
