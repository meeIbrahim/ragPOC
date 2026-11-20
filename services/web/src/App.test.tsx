import { act, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import App from "./App";
import type { WireDocument } from "./api/types";
import { POLL_INTERVAL_MS } from "./limits";
import { controlled } from "./test/fakeAsk";
import { fakeFetch, json, networkError, type FetchCall } from "./test/fakeFetch";
import { FakeXhr, installFakeXhr } from "./test/fakeXhr";
import { listBody, setVisibility, T0, wireDoc } from "./test/fixtures";

let docs: WireDocument[];
let listFails: boolean;

function routes(call: FetchCall): Response {
  if (call.url === "/documents") {
    return listFails ? networkError() : json(200, listBody(docs));
  }
  if (call.url === "/upload") {
    return json(201, { id: "u-new", filename: (call.body as { filename: string }).filename, upload_url: "http://minio/put?sig" });
  }
  if (call.url === "/upload/u-new/confirm") {
    return json(202, { status: "UPLOADED" });
  }
  throw new Error(`unexpected ${call.method} ${call.url}`);
}

async function tick(ms = 0) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms);
  });
}

function setup() {
  const s = controlled();
  const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime });
  render(<App stream={s.fn} />);
  const status = () => screen.getAllByRole("status").find((el) => el.getAttribute("aria-live") === "polite");
  return { s, user, status };
}

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(T0 + 60_000);
  installFakeXhr();
  docs = [];
  listFails = false;
  fakeFetch(routes);
  setVisibility("visible");
});
afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe("page (design 2)", () => {
  test("header: product name, subtitle and the data note", async () => {
    setup();
    await tick();
    expect(screen.getByRole("heading", { level: 1, name: "Pucho" })).toBeInTheDocument();
    expect(screen.getByText("Ask questions across your uploaded files. Answers cite their sources.")).toBeInTheDocument();
    expect(screen.getByText("Demo build · use public documents only")).toBeInTheDocument();
  });

  test("initial load: skeleton rows and a disabled form, then the first-visit states", async () => {
    setup();
    expect(screen.getByTestId("documents-loading")).toBeInTheDocument();
    expect(screen.getByText("Loading documents…")).toBeInTheDocument();
    expect(screen.getByLabelText("Question")).toBeDisabled();
    await tick();
    expect(screen.getByText("No documents yet.")).toBeInTheDocument();
    expect(screen.getByText("Upload a document to start asking questions.")).toBeInTheDocument();
  });

  test("an initial failure shows Try again in both panels", async () => {
    listFails = true;
    const { user } = setup();
    await tick();
    expect(screen.getAllByRole("button", { name: "Try again" })).toHaveLength(2);
    listFails = false;
    docs = [wireDoc({ id: "a" })];
    await user.click(screen.getAllByRole("button", { name: "Try again" })[0] as HTMLElement);
    await tick();
    expect(screen.getByRole("option", { name: /^a\.md, Indexed/ })).toBeInTheDocument();
  });
});

describe("the normal flow", () => {
  test("upload, index, select, ask, done", async () => {
    const { s, user, status } = setup();
    await tick();

    await user.upload(screen.getByTestId("file-input"), new File(["# pods"], "kubernetes-pods.md"));
    await tick();
    expect(screen.getByRole("option", { name: "kubernetes-pods.md, Uploading: 0%" })).toBeInTheDocument();
    act(() => FakeXhr.latest().progress(5, 10));
    expect(screen.getByRole("option", { name: "kubernetes-pods.md, Uploading: 50%" })).toBeInTheDocument();

    docs = [wireDoc({ id: "u-new", filename: "kubernetes-pods.md", status: "PROCESSING", created_at: new Date(T0 + 50_000).toISOString() })];
    act(() => FakeXhr.latest().respond(200));
    await tick();
    expect(screen.getByRole("option", { name: "kubernetes-pods.md, Processing: 10 s" })).toBeInTheDocument();
    expect(screen.getByText("Your documents are still being indexed. You can ask once one is ready.")).toBeInTheDocument();

    docs = [wireDoc({ id: "u-new", filename: "kubernetes-pods.md", status: "INDEXED", created_at: new Date(T0 + 50_000).toISOString() })];
    await tick(POLL_INTERVAL_MS);
    expect(status()).toHaveTextContent(/^kubernetes-pods\.md is indexed\.$/);
    const row = screen.getByRole("option", { name: /^kubernetes-pods\.md, Indexed/ });
    expect(screen.getByRole("button", { name: "Summarize kubernetes pods" })).toBeInTheDocument();

    await user.click(row);
    expect(row).toHaveAttribute("aria-selected", "true");
    expect(screen.getByText("Searching 1 document")).toBeInTheDocument();

    await user.type(screen.getByLabelText("Question"), "What is a pod?{Enter}");
    expect(s.calls[0]).toEqual({ question: "What is a pod?", ids: ["u-new"] });
    await s.push(
      { type: "sources", sources: [{ filename: "kubernetes-pods.md", snippet: "A pod is…", score: 0.8 }] },
      { type: "delta", text: "A pod is the smallest unit [1]." },
      { type: "done" },
    );
    expect(screen.getByText(/^Answered from 1 source · \d+\.\d s$/)).toBeInTheDocument();
    expect(status()).toHaveTextContent(/^Answer ready, 1 source$/);

    await user.click(screen.getByRole("button", { name: "Clear" }));
    expect(screen.getByText("Searching all indexed documents")).toBeInTheDocument();
  });

  test("a duplicate merges into the older row with a toast and a flash", async () => {
    docs = [wireDoc({ id: "old", filename: "notes.md" })];
    const { user } = setup();
    await tick();
    await user.upload(screen.getByTestId("file-input"), new File(["x"], "notes-copy.md"));
    await tick();
    docs = [wireDoc({ id: "old", filename: "notes.md", upload_ids: ["old", "u-new"] })];
    act(() => FakeXhr.latest().respond(200));
    await tick();
    const toast = screen.getByText((_, el) => el?.textContent === `"notes-copy.md" is already uploaded as "notes.md".` && el.tagName === "SPAN");
    expect(within(toast).getAllByText(/notes/)[0]).toHaveAttribute("dir", "auto");
    expect(screen.getAllByRole("option")).toHaveLength(1);
    expect(screen.getByRole("option", { name: /^notes\.md/ }).className).toMatch(/flash/);
  });

  test("a duplicate of a failed document gives a sticky error toast naming the reason", async () => {
    docs = [wireDoc({ id: "old", filename: "scan.pdf", status: "FAILED", failure_reason: "EMPTY_TEXT" })];
    const { user } = setup();
    await tick();
    await user.upload(screen.getByTestId("file-input"), new File(["x"], "scan-2.pdf"));
    await tick();
    docs = [wireDoc({ id: "old", filename: "scan.pdf", status: "FAILED", failure_reason: "EMPTY_TEXT", upload_ids: ["old", "u-new"] })];
    act(() => FakeXhr.latest().respond(200));
    await tick();
    const alert = screen.getByRole("alert");
    expect(alert).toHaveTextContent(
      `"scan-2.pdf" matches "scan.pdf", which failed: No text found in this file. Scanned PDFs need OCR, which isn't supported. Uploading it again won't retry it.`,
    );
  });

  test("the selection cap note shows under the scope line and clears on the next change", async () => {
    docs = Array.from({ length: 101 }, (_, i) => wireDoc({ id: `d${i}` }));
    const { user } = setup();
    await tick();
    for (let i = 0; i < 101; i++) {
      await user.click(screen.getByRole("option", { name: new RegExp(`^d${i}\\.md,`) }));
    }
    expect(screen.getByText("You can select up to 100 documents.")).toBeInTheDocument();
    expect(screen.getByText("Searching 100 documents")).toBeInTheDocument();
    await user.click(screen.getByRole("option", { name: /^d0\.md,/ }));
    expect(screen.queryByText("You can select up to 100 documents.")).not.toBeInTheDocument();
  });
});
