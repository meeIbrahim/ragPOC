import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { FakeXhr, installFakeXhr } from "../test/fakeXhr";
import { ApiError } from "./http";
import { putFile } from "./upload";

beforeEach(() => {
  installFakeXhr();
});
afterEach(() => {
  vi.unstubAllGlobals();
});

const file = new File(["hello"], "a.md", { type: "text/markdown" });

test("PUTs the file to the presigned URL and reports progress as a fraction", async () => {
  const progress: number[] = [];
  const done = putFile("http://minio/x?sig", file, (f) => progress.push(f));
  const xhr = FakeXhr.latest();
  expect(xhr).toMatchObject({ method: "PUT", url: "http://minio/x?sig", body: file });
  xhr.progress(2, 5);
  xhr.progress(5, 5);
  xhr.respond(200);
  await expect(done).resolves.toBeUndefined();
  expect(progress).toEqual([0.4, 1]);
});

test("a non-200 rejects with a server error that never carries the URL", async () => {
  const done = putFile("http://minio/x?X-Amz-Signature=secret", file, () => {});
  FakeXhr.latest().respond(403);
  const e = await done.catch((x: unknown) => x);
  expect(e).toBeInstanceOf(ApiError);
  expect(e).toMatchObject({ kind: "server", status: 403 });
  expect((e as ApiError).message).not.toContain("secret");
});

test("a network error rejects as network", async () => {
  const done = putFile("http://minio/x", file, () => {});
  FakeXhr.latest().fail();
  await expect(done).rejects.toMatchObject({ kind: "network" });
});

test("an already-aborted signal rejects at once", async () => {
  const ctrl = new AbortController();
  ctrl.abort();
  await expect(putFile("http://minio/x", file, () => {}, ctrl.signal)).rejects.toMatchObject({ kind: "aborted" });
});

test("abort through the signal stops the PUT", async () => {
  const ctrl = new AbortController();
  const done = putFile("http://minio/x", file, () => {}, ctrl.signal);
  ctrl.abort();
  await expect(done).rejects.toMatchObject({ kind: "aborted" });
  expect(FakeXhr.latest().aborted).toBe(true);
});
