import { act, fireEvent, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import type { Source } from "../../api/types";
import { controlled } from "../../test/fakeAsk";
import { LiveRegionProvider } from "../ui/LiveRegion";
import { ToastProvider } from "../ui/Toast";
import type { DocsReadiness } from "./AnswerArea";
import { AskPanel } from "./AskPanel";

const src = (n: number): Source => ({ filename: `f${n}.md`, snippet: `s${n}`, score: 0.5 });

type Props = Parameters<typeof AskPanel>[0];

function setup(over: Partial<Props> = {}) {
  const s = controlled();
  const props: Props = {
    docs: { kind: "ready" },
    selectedIds: [],
    onClearSelection: vi.fn(),
    capError: undefined,
    suggestions: ["Summarize kubernetes pods", "Key points of rust ownership"],
    onRefreshDocuments: vi.fn(),
    stream: s.fn,
    ...over,
  };
  const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime });
  const view = render(
    <LiveRegionProvider>
      <ToastProvider>
        <AskPanel {...props} />
      </ToastProvider>
    </LiveRegionProvider>,
  );
  const box = () => screen.getByLabelText("Question");
  const status = () => screen.getAllByRole("status").find((el) => el.getAttribute("aria-live") === "polite");
  const askQuestion = async (q: string) => {
    await user.type(box(), q);
    await user.keyboard("{Enter}");
  };
  return { ...view, s, props, user, box, status, askQuestion };
}

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(1_000_000);
});
afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe("before a question (design 5.2, page states)", () => {
  test.each<[DocsReadiness, string, string]>([
    [{ kind: "loading" }, "", "Loading documents…"],
    [{ kind: "none" }, "Upload a document to start asking questions.", "Upload a document first."],
    [
      { kind: "noneIndexed", processing: 2 },
      "Your documents are still being indexed. You can ask once one is ready.",
      "Waiting for a document to finish indexing.",
    ],
  ])("%j disables the form with its reason", (docs, message, reason) => {
    const { box } = setup({ docs });
    if (message) {
      expect(screen.getByText(message)).toBeInTheDocument();
    }
    expect(screen.getByText(reason)).toBeInTheDocument();
    expect(box()).toBeDisabled();
  });

  test("none indexed shows the processing count", () => {
    setup({ docs: { kind: "noneIndexed", processing: 2 } });
    expect(screen.getByText("2 processing")).toBeInTheDocument();
  });

  test("a failed first load offers Try again here too", async () => {
    const { user, props } = setup({ docs: { kind: "failed" } });
    await user.click(screen.getByRole("button", { name: "Try again" }));
    expect(props.onRefreshDocuments).toHaveBeenCalled();
  });

  test("a suggestion chip fills the textarea and does not send", async () => {
    const { user, box, s } = setup();
    await user.click(screen.getByRole("button", { name: "Summarize kubernetes pods" }));
    expect(box()).toHaveValue("Summarize kubernetes pods");
    expect(box()).toHaveFocus();
    expect(s.calls).toHaveLength(0);
  });
});

describe("one question, start to finish", () => {
  test("sending, sources, streaming, done: copy, announcements, cleared question, focus", async () => {
    const { s, box, status, askQuestion } = setup({ selectedIds: ["u1", "u2"] });
    await askQuestion("Who owns a value?");
    expect(s.calls[0]).toEqual({ question: "Who owns a value?", ids: ["u1", "u2"] });
    expect(screen.getByText("Searching documents…")).toBeInTheDocument();
    expect(screen.getByText("Who owns a value?", { selector: "p" })).toBeInTheDocument();
    expect(status()).toHaveTextContent("Searching documents");
    expect(screen.getByRole("button", { name: "Stop" })).toBeInTheDocument();

    await s.push({ type: "sources", sources: [src(1), src(2)] });
    expect(screen.getByRole("heading", { name: "Sources" })).toBeInTheDocument();
    expect(screen.getByText("Writing answer…")).toBeInTheDocument();

    await s.push({ type: "delta", text: "Each value has one owner [1]." });
    expect(screen.getByRole("button", { name: "Source 1: f1.md" })).toBeInTheDocument();
    expect(screen.getByText("▍")).toBeInTheDocument();
    expect(status()).toHaveTextContent("Answer started");
    expect(screen.getByTestId("answer-text").closest("[aria-live]"), "the answer region is never live").toBeNull();

    vi.setSystemTime(1_006_200);
    await s.push({ type: "done" });
    expect(screen.queryByText("▍")).not.toBeInTheDocument();
    expect(screen.getByText("Answered from 2 sources · 6.2 s")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Copy answer" })).toBeInTheDocument();
    expect(status()).toHaveTextContent("Answer ready, 2 sources");
    expect(box()).toHaveValue("");
    expect(box()).toHaveFocus();
  });

  test("Done leaves focus alone when the reader is in the sources", async () => {
    const { s, user, askQuestion } = setup();
    await askQuestion("q");
    await s.push({ type: "sources", sources: [src(1)] }, { type: "delta", text: "a" });
    const card = within(screen.getByTestId("source-1")).getByRole("button");
    await user.click(card);
    await s.push({ type: "done" });
    expect(card).toHaveFocus();
  });

  test("Copy answer writes the answer and toasts", async () => {
    const { s, user, askQuestion } = setup();
    // user-event installs its own clipboard at setup(); spy on that one.
    const writeText = vi.spyOn(navigator.clipboard, "writeText");
    await askQuestion("q");
    await s.push({ type: "sources", sources: [src(1)] }, { type: "delta", text: "the answer" }, { type: "done" });
    await user.click(screen.getByRole("button", { name: "Copy answer" }));
    expect(writeText).toHaveBeenCalledWith("the answer");
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(screen.getByText("Answer copied.")).toBeInTheDocument();
  });
});

describe("nothing relevant (design 5.2)", () => {
  test.each([
    [[], "Try different wording, or select other documents."],
    [["u1"], "Try different wording, or clear the selection."],
  ])("selection %j gives the hint %s; the question stays", async (ids, hint) => {
    const { s, box, askQuestion } = setup({ selectedIds: ids });
    await askQuestion("q");
    await s.push({ type: "sources", sources: [] }, { type: "delta", text: "No relevant…" }, { type: "done" });
    expect(screen.getByText("No relevant information was found in the indexed documents.")).toBeInTheDocument();
    expect(screen.getByText(hint)).toBeInTheDocument();
    expect(screen.queryByRole("heading", { name: "Sources" })).not.toBeInTheDocument();
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
    expect(box()).toHaveValue("q");
  });
});

describe("stop (design 5.1, 5.2)", () => {
  test("the Stop button keeps the partial answer and restores the question", async () => {
    const { s, user, box, status, askQuestion } = setup();
    await askQuestion("why?");
    await s.push({ type: "sources", sources: [src(1)] }, { type: "delta", text: "partial" });
    await user.click(screen.getByRole("button", { name: "Stop" }));
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(screen.getByText("Stopped.")).toBeInTheDocument();
    expect(screen.getByTestId("answer-text")).toHaveTextContent("partial");
    expect(screen.getByRole("heading", { name: "Sources" })).toBeInTheDocument();
    expect(box()).toHaveValue("why?");
    expect(status()).toHaveTextContent("Stopped");
    expect(screen.getByRole("button", { name: "Ask" })).toBeInTheDocument();
  });

  test("Escape anywhere in the panel stops the stream", async () => {
    const { s, askQuestion } = setup();
    await askQuestion("why?");
    await s.push({ type: "sources", sources: [src(1)] });
    fireEvent.keyDown(screen.getByRole("heading", { name: "Sources" }), { key: "Escape" });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(screen.getByText("Stopped.")).toBeInTheDocument();
  });
});

describe("errors (design 5.2)", () => {
  test("UNAVAILABLE: the card, focus on Try again, which resends the same question and scope", async () => {
    const { s, user, box, askQuestion } = setup({ selectedIds: ["u9"] });
    await askQuestion("why?");
    await s.push({ type: "sources", sources: [src(1)] }, { type: "delta", text: "half" }, { type: "error", code: "UNAVAILABLE", message: "x" });
    const card = screen.getByRole("alert");
    expect(card).toHaveTextContent("The answer service is unavailable.");
    expect(card).toHaveTextContent("Try again in a moment.");
    expect(screen.getByTestId("answer-text")).toHaveTextContent("half");
    const tryAgain = within(card).getByRole("button", { name: "Try again" });
    expect(tryAgain).toHaveFocus();
    expect(box()).toHaveValue("why?");
    await user.click(tryAgain);
    expect(s.calls).toEqual([
      { question: "why?", ids: ["u9"] },
      { question: "why?", ids: ["u9"] },
    ]);
  });

  test("another code shows the server message; a lost connection has its own title", async () => {
    const { s, user, askQuestion } = setup();
    await askQuestion("q");
    await s.push({ type: "error", code: "INTERNAL", message: "rag service failed" });
    expect(screen.getByRole("alert")).toHaveTextContent("Something went wrong while answering.");
    expect(screen.getByRole("alert")).toHaveTextContent("rag service failed");
    await user.click(screen.getByRole("button", { name: "Try again" }));
    await s.push({ type: "connectionLost" });
    expect(screen.getByRole("alert")).toHaveTextContent("The connection was lost.");
  });

  test("a 400 about the question shows under the textarea; one about the selection under the scope line", async () => {
    const { s, user, askQuestion } = setup({ selectedIds: ["u1"] });
    await askQuestion("q");
    await s.push({ type: "invalid", message: "question is too long" });
    expect(screen.getByLabelText("Question")).toHaveAttribute("aria-describedby", "question-error");
    expect(screen.getByRole("alert")).toHaveTextContent("question is too long");
    await user.keyboard("{Enter}");
    await s.push({ type: "invalid", message: "at most 100 upload_ids may be selected" });
    expect(document.getElementById("scope-error")).toHaveTextContent("at most 100 upload_ids may be selected");
  });

  test("the cap error from the documents panel shows under the scope line", () => {
    setup({ capError: "You can select up to 100 documents.", selectedIds: ["u1"] });
    expect(document.getElementById("scope-error")).toHaveTextContent("You can select up to 100 documents.");
  });
});

describe("follow-scroll (design 2)", () => {
  test("scrolling up while streaming shows Jump to latest; it returns to the bottom", async () => {
    const { s, user, askQuestion } = setup();
    await askQuestion("q");
    await s.push({ type: "sources", sources: [src(1)] }, { type: "delta", text: "a" });
    const scroller = screen.getByTestId("answer-scroller");
    Object.defineProperty(scroller, "scrollHeight", { configurable: true, value: 1000 });
    Object.defineProperty(scroller, "clientHeight", { configurable: true, value: 200 });
    scroller.scrollTop = 100;
    fireEvent.scroll(scroller);
    const jump = screen.getByRole("button", { name: "Jump to latest" });
    await user.click(jump);
    expect(scroller.scrollTop).toBe(1000);
    expect(screen.queryByRole("button", { name: "Jump to latest" })).not.toBeInTheDocument();
    scroller.scrollTop = 760;
    fireEvent.scroll(scroller);
    expect(screen.queryByRole("button", { name: "Jump to latest" }), "within 48 of the bottom keeps following").not.toBeInTheDocument();
  });
});
