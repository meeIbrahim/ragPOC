import { fireEvent, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, test, vi } from "vitest";
import { fromDocument, type DocumentEntry, type WireDocument } from "../../api/types";
import type { LocalRow } from "../../hooks/useUploads";
import { MAX_SELECTED } from "../../limits";
import { T0, wireDoc } from "../../test/fixtures";
import { DocumentsPanel } from "./DocumentsPanel";

const MIN = 60_000;

function entry(id: string, over: Partial<WireDocument> = {}): DocumentEntry {
  return fromDocument(wireDoc({ id, ...over }));
}

type Props = Parameters<typeof DocumentsPanel>[0];

function renderPanel(over: Partial<Props> = {}) {
  const props: Props = {
    entries: [],
    localRows: [],
    load: "ready",
    poll: "ok",
    truncated: false,
    now: T0 + 2 * MIN,
    selected: new Set(),
    onSelectionChange: vi.fn(),
    onCapExceeded: vi.fn(),
    onRefresh: vi.fn(),
    uploadError: undefined,
    onFiles: vi.fn(),
    onRetryUpload: vi.fn(),
    onDismissUpload: vi.fn(),
    flash: undefined,
    ...over,
  };
  return { ...render(<DocumentsPanel {...props} />), props };
}

describe("panel states (design 2, 3.6)", () => {
  test("initial load shows skeleton rows", () => {
    renderPanel({ load: "loading" });
    expect(screen.getByTestId("documents-loading").children).toHaveLength(4);
  });

  test("a failed first load offers Try again", async () => {
    const { props } = renderPanel({ load: "failed" });
    expect(screen.getByRole("alert")).toHaveTextContent("Couldn't reach the server.");
    await userEvent.click(screen.getByRole("button", { name: "Try again" }));
    expect(props.onRefresh).toHaveBeenCalled();
  });

  test("empty state copy", () => {
    renderPanel();
    expect(screen.getByText("No documents yet.")).toBeInTheDocument();
    expect(
      screen.getByText("Upload a PDF, TXT or MD file. Short files index fastest; long PDFs can take several minutes."),
    ).toBeInTheDocument();
  });

  test("truncated note counts the listed entries", () => {
    renderPanel({ entries: [entry("a"), entry("b")], truncated: true });
    expect(screen.getByText("Showing the newest 2 documents.")).toBeInTheDocument();
  });

  test("poll errors keep rows: Reconnecting… then Can't reach the server with Retry now", async () => {
    const { rerender, props } = renderPanel({ entries: [entry("a")], poll: "reconnecting" });
    expect(screen.getByText("Reconnecting…")).toBeInTheDocument();
    rerender(<DocumentsPanel {...props} poll="stale" />);
    expect(screen.getByText(/Can't reach the server\. Status may be out of date\./)).toBeInTheDocument();
    expect(screen.getAllByRole("option")).toHaveLength(1);
    await userEvent.click(screen.getByRole("button", { name: "Retry now" }));
    expect(props.onRefresh).toHaveBeenCalled();
  });
});

describe("row states (design 3.2, exact copy)", () => {
  test("local rows: queued counts, upload percent, failed with Retry and Dismiss", async () => {
    const localRows: LocalRow[] = [
      { key: 4, filename: "d.md", phase: "failed" },
      { key: 3, filename: "c.md", phase: "queued", ahead: 2 },
      { key: 2, filename: "b.md", phase: "queued", ahead: 1 },
      { key: 1, filename: "a.md", phase: "uploading", progress: 0.454 },
    ];
    const { props } = renderPanel({ localRows });
    const option = (name: string) => screen.getByRole("option", { name: new RegExp(`^${name}`) });
    expect(screen.getByRole("option", { name: "c.md, Queued: Waiting for 2 uploads" })).toBeInTheDocument();
    expect(screen.getByRole("option", { name: "b.md, Queued: Waiting for 1 upload" })).toBeInTheDocument();
    expect(screen.getByRole("option", { name: "a.md, Uploading: 45%" })).toBeInTheDocument();
    const failed = option("d.md");
    expect(failed).toHaveTextContent("Upload didn't finish. Try again.");
    await userEvent.click(within(failed).getByRole("button", { name: "Retry" }));
    expect(props.onRetryUpload).toHaveBeenCalledWith(4);
    await userEvent.click(within(failed).getByRole("button", { name: "Dismiss" }));
    expect(props.onDismissUpload).toHaveBeenCalledWith(4);
    for (const o of screen.getAllByRole("option")) {
      expect(o, "local rows are never selectable").toHaveAttribute("aria-disabled", "true");
    }
  });

  test("a confirmed but unlisted upload reads Taking longer than expected", () => {
    renderPanel({ localRows: [{ key: 1, filename: "a.md", phase: "processing", uploadId: "u1", notListed: true }] });
    expect(screen.getByRole("option", { name: "a.md, Processing: Taking longer than expected" })).toBeInTheDocument();
  });

  test("server rows: processing elapsed, slow, indexed relative time, failed reason with the code in title", () => {
    renderPanel({
      now: T0 + 6 * MIN,
      entries: [
        entry("p", { status: "PROCESSING", created_at: new Date(T0 + 5 * MIN + 20_000).toISOString() }),
        entry("slow", { status: "PROCESSING" }),
        entry("ok", { status: "INDEXED", created_at: new Date(T0 + 4 * MIN).toISOString() }),
        entry("bad", { status: "FAILED", filename: "scan.pdf", failure_reason: "EMPTY_TEXT" }),
        entry("ahead", { status: "PROCESSING", created_at: new Date(T0 + 7 * MIN).toISOString() }),
      ],
    });
    expect(screen.getByRole("option", { name: "p.md, Processing: 40 s" })).toBeInTheDocument();
    expect(screen.getByRole("option", { name: "slow.md, Processing: Taking longer than usual" })).toBeInTheDocument();
    expect(screen.getByRole("option", { name: "ok.md, Indexed: 2 min ago" })).toBeInTheDocument();
    expect(screen.getByRole("option", { name: "ahead.md, Processing: 0 s" }), "clock skew clamps at 0 (T6)").toBeInTheDocument();
    const failed = screen.getByRole("option", { name: "scan.pdf, Failed: No text found in this file. Scanned PDFs need OCR, which isn't supported." });
    expect(within(failed).getByTitle("EMPTY_TEXT")).toBeInTheDocument();
  });

  test("filenames carry dir=auto and the full name in title (T5)", () => {
    renderPanel({ entries: [entry("u", { filename: "رپورٹ-۲۰۲۶-حتمی-نسخہ-جائزہ.pdf" })] });
    const name = screen.getByTitle("رپورٹ-۲۰۲۶-حتمی-نسخہ-جائزہ.pdf");
    expect(name).toHaveAttribute("dir", "auto");
  });

  test("only indexed rows are selectable; the selected one is marked", () => {
    renderPanel({
      entries: [entry("a"), entry("b", { status: "PROCESSING" }), entry("c", { status: "FAILED" })],
      selected: new Set(["a"]),
    });
    expect(screen.getByRole("option", { name: /^a\.md/ })).toHaveAttribute("aria-selected", "true");
    expect(screen.getByRole("option", { name: /^b\.md/ })).toHaveAttribute("aria-disabled", "true");
    expect(screen.getByRole("option", { name: /^c\.md/ })).toHaveAttribute("aria-disabled", "true");
  });

  test("the merge flash marks the absorbing row", () => {
    renderPanel({ entries: [entry("a"), entry("b")], flash: { id: "b", n: 1 } });
    expect(screen.getByRole("option", { name: /^b\.md/ }).className).toMatch(/flash/);
    expect(screen.getByRole("option", { name: /^a\.md/ }).className).not.toMatch(/flash/);
  });
});

describe("selection and keyboard (design 3.2, 4)", () => {
  const three = [entry("a"), entry("b", { status: "PROCESSING" }), entry("c")];

  test("clicking an indexed row toggles it; a processing row does nothing", async () => {
    const { props } = renderPanel({ entries: three });
    await userEvent.click(screen.getByRole("option", { name: /^a\.md/ }));
    expect(props.onSelectionChange).toHaveBeenLastCalledWith(new Set(["a"]));
    await userEvent.click(screen.getByRole("option", { name: /^b\.md/ }));
    expect(props.onSelectionChange).toHaveBeenCalledTimes(1);
  });

  test("one tab stop; Down, Up, Home, End move the active row; Space toggles", async () => {
    const user = userEvent.setup();
    const { props } = renderPanel({ entries: three, selected: new Set(["c"]) });
    await user.tab();
    await user.tab();
    const list = screen.getByRole("listbox");
    expect(list).toHaveFocus();
    expect(list).toHaveAttribute("aria-activedescendant", "doc-a");
    await user.keyboard("{ArrowDown}");
    expect(list).toHaveAttribute("aria-activedescendant", "doc-b");
    await user.keyboard(" ");
    expect(props.onSelectionChange, "Space on a processing row does nothing").not.toHaveBeenCalled();
    await user.keyboard("{End}");
    expect(list).toHaveAttribute("aria-activedescendant", "doc-c");
    await user.keyboard(" ");
    expect(props.onSelectionChange).toHaveBeenLastCalledWith(new Set());
    await user.keyboard("{Home}");
    expect(list).toHaveAttribute("aria-activedescendant", "doc-a");
    await user.keyboard("{ArrowUp}");
    expect(list).toHaveAttribute("aria-activedescendant", "doc-a");
  });

  test(`checking row ${MAX_SELECTED + 1} is refused through onCapExceeded`, async () => {
    const many = Array.from({ length: MAX_SELECTED + 1 }, (_, i) => entry(`d${i}`));
    const selected = new Set(many.slice(0, MAX_SELECTED).map((e) => e.id));
    const { props } = renderPanel({ entries: many, selected });
    fireEvent.click(screen.getByRole("option", { name: new RegExp(`^d${MAX_SELECTED}\\.md`) }));
    expect(props.onCapExceeded).toHaveBeenCalledTimes(1);
    expect(props.onSelectionChange).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole("option", { name: /^d0\.md/ }));
    expect(props.onSelectionChange, "unchecking still works at the cap").toHaveBeenCalledTimes(1);
  });
});

describe("upload control (design 3.1)", () => {
  test("picking files hands them over; the inline error is tied to the button", async () => {
    const { props, rerender } = renderPanel();
    const f = new File(["x"], "a.md");
    await userEvent.upload(screen.getByTestId("file-input"), f);
    expect(props.onFiles).toHaveBeenCalledWith([f]);
    rerender(<DocumentsPanel {...props} uploadError={`"photo.png" isn't supported. Use PDF, TXT or MD.`} />);
    expect(screen.getByRole("alert")).toHaveTextContent(`"photo.png" isn't supported. Use PDF, TXT or MD.`);
    expect(screen.getByRole("button", { name: "Upload file" })).toHaveAttribute("aria-describedby", "upload-error");
  });

  test("drag-over says Drop to upload; dropping hands the files over", () => {
    const { props } = renderPanel();
    const zone = screen.getByTestId("dropzone");
    fireEvent.dragOver(zone);
    expect(screen.getByText("Drop to upload")).toBeInTheDocument();
    const f = new File(["x"], "a.pdf");
    fireEvent.drop(zone, { dataTransfer: { files: [f] } });
    expect(props.onFiles).toHaveBeenCalledWith([f]);
    expect(screen.queryByText("Drop to upload")).not.toBeInTheDocument();
  });
});
