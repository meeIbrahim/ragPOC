import { act, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import { fromDocument, type DocumentEntry, type WireDocument } from "../api/types";
import { MAX_FILE_BYTES, NOT_LISTED_AFTER_POLLS } from "../limits";
import { fakeFetch, json, text, type FetchCall } from "../test/fakeFetch";
import { FakeXhr, installFakeXhr } from "../test/fakeXhr";
import { setVisibility, wireDoc } from "../test/fixtures";
import { useUploads, type Merge } from "./useUploads";

const MB = 1_048_576;

function file(name: string, size?: number): File {
  const f = new File(["x"], name);
  if (size !== undefined) {
    Object.defineProperty(f, "size", { value: size });
  }
  return f;
}

type Props = { entries: DocumentEntry[]; version: number };

let routes: (call: FetchCall) => Response | Promise<Response>;
let created = 0;

function api(call: FetchCall) {
  return routes(call);
}

function defaultRoutes(call: FetchCall): Response {
  if (call.url === "/upload") {
    created += 1;
    const body = call.body as { filename: string };
    return json(201, { id: `u${created}`, filename: body.filename, upload_url: `http://minio/u${created}?sig` });
  }
  if (call.url.endsWith("/confirm")) {
    return json(202, { status: "UPLOADED" });
  }
  throw new Error(`unexpected ${call.method} ${call.url}`);
}

function setup() {
  const onConfirmed = vi.fn();
  const onMerged = vi.fn<(m: Merge) => void>();
  const hook = renderHook<ReturnType<typeof useUploads>, Props>((p) => useUploads({ ...p, onConfirmed, onMerged }), {
    initialProps: { entries: [], version: 0 },
  });
  let version = 0;
  const poll = (docs: DocumentEntry[]) => {
    version += 1;
    act(() => hook.rerender({ entries: docs, version }));
  };
  return { ...hook, onConfirmed, onMerged, poll };
}

async function flush() {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(0);
  });
}

/** Drives the current upload's PUT to 200 and lets confirm resolve. */
async function finishPut() {
  act(() => FakeXhr.latest().respond(200));
  await flush();
}

function entry(id: string, over: Partial<WireDocument> = {}) {
  return fromDocument(wireDoc({ id, ...over }));
}

beforeEach(() => {
  vi.useFakeTimers();
  installFakeXhr();
  created = 0;
  routes = defaultRoutes;
  fakeFetch(api);
  setVisibility("visible");
});
afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe("queue", () => {
  test("one upload at a time in pick order; the others wait with a count (design 3.2)", async () => {
    const { result } = setup();
    act(() => result.current.add([file("a.md"), file("b.md"), file("c.md")]));
    await flush();
    const byName = Object.fromEntries(result.current.rows.map((r) => [r.filename, r]));
    expect(byName["a.md"]).toMatchObject({ phase: "uploading", progress: 0 });
    expect(byName["b.md"]).toMatchObject({ phase: "queued", ahead: 1 });
    expect(byName["c.md"]).toMatchObject({ phase: "queued", ahead: 2 });
    expect(result.current.rows.map((r) => r.filename), "newest pick on top").toEqual(["c.md", "b.md", "a.md"]);
    expect(FakeXhr.instances).toHaveLength(1);
  });

  test("progress updates, confirm makes the row processing and starts the next", async () => {
    const { result, onConfirmed } = setup();
    act(() => result.current.add([file("a.md"), file("b.md")]));
    await flush();
    act(() => FakeXhr.latest().progress(1, 4));
    expect(result.current.rows.find((r) => r.filename === "a.md")).toMatchObject({ progress: 0.25 });
    await finishPut();
    expect(result.current.rows.find((r) => r.filename === "a.md")).toMatchObject({ phase: "processing", uploadId: "u1" });
    expect(onConfirmed).toHaveBeenCalledTimes(1);
    expect(result.current.rows.find((r) => r.filename === "b.md")).toMatchObject({ phase: "uploading" });
    expect(FakeXhr.instances).toHaveLength(2);
    expect(result.current.active).toBe(true);
  });

  test("a poll while the PUT is in flight keeps the local row (edge case 6)", async () => {
    const { result, poll } = setup();
    act(() => result.current.add([file("a.md")]));
    await flush();
    poll([entry("other")]);
    expect(result.current.rows).toHaveLength(1);
    expect(result.current.rows[0]).toMatchObject({ phase: "uploading" });
  });

  test("a hidden tab does not pause the PUT (edge case 8)", async () => {
    const { result } = setup();
    act(() => result.current.add([file("a.md")]));
    await flush();
    act(() => setVisibility("hidden"));
    await finishPut();
    expect(result.current.rows[0]).toMatchObject({ phase: "processing" });
  });
});

describe("matching the server list", () => {
  async function confirmed() {
    const s = setup();
    act(() => s.result.current.add([file("notes-copy.md")]));
    await flush();
    await finishPut();
    return s;
  }

  test("its own entry replaces the local row with no merge", async () => {
    const { result, poll, onMerged } = await confirmed();
    poll([entry("u1", { status: "INDEXED" })]);
    expect(result.current.rows).toEqual([]);
    expect(onMerged).not.toHaveBeenCalled();
    expect(result.current.active).toBe(false);
  });

  test("an older entry listing the upload id is a merge", async () => {
    const { result, poll, onMerged } = await confirmed();
    const older = entry("old", { filename: "notes.md", upload_ids: ["old", "u1"] });
    poll([older]);
    expect(result.current.rows).toEqual([]);
    expect(onMerged).toHaveBeenCalledWith({ filename: "notes-copy.md", entry: older });
  });

  test.each(["INDEXED", "FAILED"] as const)(
    "delayed merge: own PROCESSING entry first, then an older %s entry (design 3.3 as amended)",
    async (status) => {
      const { result, poll, onMerged } = await confirmed();
      poll([entry("u1", { status: "PROCESSING" })]);
      expect(result.current.rows, "the server row stands in for the local one").toEqual([]);
      expect(onMerged).not.toHaveBeenCalled();
      const older = entry("old", { status, failure_reason: status === "FAILED" ? "EMPTY_TEXT" : undefined, upload_ids: ["old", "u1"] });
      poll([older]);
      expect(onMerged).toHaveBeenCalledTimes(1);
      expect(onMerged.mock.calls[0]?.[0].entry.status).toBe(status);
    },
  );

  test("a watched upload whose entry leaves the list stops being watched", async () => {
    const { result, poll, onMerged } = await confirmed();
    poll([entry("u1", { status: "PROCESSING" })]);
    poll([]);
    poll([entry("old", { upload_ids: ["old", "u1"] })]);
    expect(onMerged).not.toHaveBeenCalled();
    expect(result.current.active).toBe(false);
  });

  test("an own entry that reaches INDEXED stops the watch", async () => {
    const { poll, onMerged } = await confirmed();
    poll([entry("u1", { status: "PROCESSING" })]);
    poll([entry("u1", { status: "INDEXED" })]);
    poll([entry("old", { upload_ids: ["old", "u1"] })]);
    expect(onMerged).not.toHaveBeenCalled();
  });

  test(`${NOT_LISTED_AFTER_POLLS} polls with no match mark the row not listed; it changes once listed`, async () => {
    const { result, poll } = await confirmed();
    for (let i = 0; i < NOT_LISTED_AFTER_POLLS - 1; i++) {
      poll([]);
    }
    expect(result.current.rows[0]).toMatchObject({ notListed: false });
    poll([]);
    expect(result.current.rows[0]).toMatchObject({ phase: "processing", notListed: true });
    poll([entry("u1", { status: "PROCESSING" })]);
    expect(result.current.rows).toEqual([]);
  });
});

describe("failures before confirm (design 3.3 step 5)", () => {
  test("a PUT failure gives Retry with the same File, and Dismiss", async () => {
    const { result } = setup();
    const f = file("a.md");
    act(() => result.current.add([f]));
    await flush();
    act(() => FakeXhr.latest().fail());
    await flush();
    const row = result.current.rows[0];
    expect(row).toMatchObject({ phase: "failed" });
    act(() => result.current.retry(row?.key ?? -1));
    await flush();
    expect(FakeXhr.instances).toHaveLength(2);
    expect(FakeXhr.latest().body, "Retry re-sends the same File object").toBe(f);
    act(() => FakeXhr.latest().fail());
    await flush();
    act(() => result.current.dismiss(result.current.rows[0]?.key ?? -1));
    expect(result.current.rows).toEqual([]);
  });

  test("a create failure (5xx) and a confirm 410 both give a failed row", async () => {
    routes = (call) => (call.url === "/upload" ? text(500, "internal error") : defaultRoutes(call));
    const { result } = setup();
    act(() => result.current.add([file("a.md")]));
    await flush();
    expect(result.current.rows[0]).toMatchObject({ phase: "failed" });

    routes = (call) => (call.url.endsWith("/confirm") ? text(410, "upload intent expired") : defaultRoutes(call));
    act(() => result.current.dismiss(result.current.rows[0]?.key ?? -1));
    act(() => result.current.add([file("b.md")]));
    await flush();
    await finishPut();
    expect(result.current.rows[0]).toMatchObject({ filename: "b.md", phase: "failed" });
  });
});

describe("client checks and server 400s (design 3.1)", () => {
  test("an unsupported extension never becomes a row or a request; case-insensitive", async () => {
    const { result } = setup();
    act(() => result.current.add([file("photo.PNG")]));
    await flush();
    expect(result.current.rows).toEqual([]);
    expect(result.current.error).toBe(`"photo.PNG" isn't supported. Use PDF, TXT or MD.`);
    expect(vi.mocked(fetch)).not.toHaveBeenCalled();
    act(() => result.current.add([file("REPORT.PDF")]));
    await flush();
    expect(result.current.error, "the next pick clears the error").toBeUndefined();
    expect(result.current.rows[0]).toMatchObject({ filename: "REPORT.PDF" });
  });

  test("a file over MAX_FILE_BYTES is rejected before any request; exactly the limit is accepted", async () => {
    const { result } = setup();
    act(() => result.current.add([file("big.pdf", 40 * MB)]));
    expect(result.current.error).toBe(`"big.pdf" is 40 MB. The limit is 10 MB.`);
    expect(result.current.rows).toEqual([]);
    act(() => result.current.add([file("edge.pdf", MAX_FILE_BYTES)]));
    expect(result.current.error).toBeUndefined();
    expect(result.current.rows).toHaveLength(1);
  });

  test("a server 400 for the extension shows the client copy and leaves no row", async () => {
    routes = () => text(400, "unsupported file extension: .md");
    const { result } = setup();
    act(() => result.current.add([file("a.md")]));
    await flush();
    expect(result.current.rows).toEqual([]);
    expect(result.current.error).toBe(`"a.md" isn't supported. Use PDF, TXT or MD.`);
  });

  test("any other server 400 shows its trimmed text and leaves no row", async () => {
    routes = () => text(400, "filename is required");
    const { result } = setup();
    act(() => result.current.add([file("a.md")]));
    await flush();
    expect(result.current.rows).toEqual([]);
    expect(result.current.error).toBe("filename is required");
  });
});

test("unmount aborts the PUT", async () => {
  const { result, unmount } = setup();
  act(() => result.current.add([file("a.md")]));
  await flush();
  unmount();
  expect(FakeXhr.latest().aborted).toBe(true);
});
