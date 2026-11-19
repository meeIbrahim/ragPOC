import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { createRef, useRef, useState } from "react";
import { describe, expect, test, vi } from "vitest";
import type { Source } from "../../api/types";
import { AnswerText } from "./AnswerText";
import { SourceList, type SourceListHandle } from "./SourceList";

const sources: Source[] = [
  { filename: "rust-ownership.md", snippet: "Each value has one owner.", score: 0.713 },
  { filename: "kubernetes-pods.md", snippet: "<img src=x onerror=alert(1)>", score: 0.5 },
];

function Linked({ text }: { text: string }) {
  const [hl, setHl] = useState<number | undefined>();
  const list = useRef<SourceListHandle>(null);
  return (
    <>
      <SourceList ref={list} sources={sources} highlighted={hl} />
      <AnswerText
        text={text}
        sourceNames={sources.map((s) => s.filename)}
        streaming={false}
        onHighlight={(n) => {
          setHl(n);
          if (n) {
            list.current?.reveal(n, false);
          }
        }}
        onPillClick={(n) => list.current?.reveal(n, true)}
      />
    </>
  );
}

describe("SourceList and SourceCard (design 5.3)", () => {
  test("compact rows: pill, filename with dir=auto, two-decimal score titled relevance; rank 1 outlined", () => {
    render(<SourceList sources={sources} highlighted={undefined} />);
    expect(screen.getByRole("heading", { name: "Sources" })).toBeInTheDocument();
    const first = screen.getByTestId("source-1");
    expect(first).toHaveTextContent("[1]rust-ownership.md0.71");
    expect(screen.getByTitle("rust-ownership.md")).toHaveAttribute("dir", "auto");
    expect(screen.getAllByTitle("relevance")[0]).toHaveTextContent("0.71");
    expect(first.className).toMatch(/first/);
    expect(screen.getByTestId("source-2").className).not.toMatch(/first/);
  });

  test("click and Enter expand the snippet; several can be open", async () => {
    const user = userEvent.setup();
    render(<SourceList sources={sources} highlighted={undefined} />);
    const [one, two] = screen.getAllByRole("button");
    expect(screen.queryByText("Each value has one owner.")).not.toBeInTheDocument();
    await user.click(one as HTMLElement);
    expect(one).toHaveAttribute("aria-expanded", "true");
    expect(screen.getByText("Each value has one owner.")).toBeInTheDocument();
    (two as HTMLElement).focus();
    await user.keyboard("{Enter}");
    expect(two).toHaveAttribute("aria-expanded", "true");
    expect(screen.getByText("Each value has one owner.")).toBeInTheDocument();
  });

  test("a snippet with HTML renders as literal text", async () => {
    render(<SourceList sources={sources} highlighted={undefined} />);
    await userEvent.click(screen.getAllByRole("button")[1] as HTMLElement);
    expect(screen.getByText("<img src=x onerror=alert(1)>")).toBeInTheDocument();
    expect(document.querySelector("img")).toBeNull();
  });
});

describe("AnswerText (design 5.3, T3, T4)", () => {
  const names = sources.map((s) => s.filename);

  test("in-range [n] become pill buttons labelled with the source; out of range stays text", () => {
    render(<AnswerText text="Owners [1], pods [2], made up [3]." sourceNames={names} streaming={false} onHighlight={() => {}} onPillClick={() => {}} />);
    expect(screen.getByRole("button", { name: "Source 1: rust-ownership.md" })).toHaveTextContent("[1]");
    expect(screen.getByRole("button", { name: "Source 2: kubernetes-pods.md" })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Source 3/ })).not.toBeInTheDocument();
    expect(screen.getByTestId("answer-text")).toHaveTextContent("made up [3].");
  });

  test("markdown stays literal and the text block keeps line breaks", () => {
    render(<AnswerText text={"**bold**\n# title"} sourceNames={names} streaming={false} onHighlight={() => {}} onPillClick={() => {}} />);
    const el = screen.getByTestId("answer-text");
    expect(el.textContent).toBe("**bold**\n# title");
    expect(el.querySelector("strong, h1")).toBeNull();
    expect(el.className).toMatch(/answer/);
  });

  test("HTML in the answer renders as literal text", () => {
    render(<AnswerText text="<img src=x onerror=alert(1)>" sourceNames={names} streaming={false} onHighlight={() => {}} onPillClick={() => {}} />);
    expect(document.querySelector("img")).toBeNull();
    expect(screen.getByTestId("answer-text")).toHaveTextContent("<img src=x onerror=alert(1)>");
  });

  test("a citation split across deltas renders as one pill once accumulated (T4)", () => {
    const { rerender } = render(<AnswerText text="see [1" sourceNames={names} streaming onHighlight={() => {}} onPillClick={() => {}} />);
    expect(screen.queryByRole("button")).not.toBeInTheDocument();
    rerender(<AnswerText text="see [1] and" sourceNames={names} streaming onHighlight={() => {}} onPillClick={() => {}} />);
    expect(screen.getAllByRole("button")).toHaveLength(1);
  });

  test("the caret shows only while streaming", () => {
    const { rerender } = render(<AnswerText text="x" sourceNames={names} streaming onHighlight={() => {}} onPillClick={() => {}} />);
    expect(screen.getByText("▍")).toHaveAttribute("aria-hidden", "true");
    rerender(<AnswerText text="x" sourceNames={names} streaming={false} onHighlight={() => {}} onPillClick={() => {}} />);
    expect(screen.queryByText("▍")).not.toBeInTheDocument();
  });
});

describe("pills and cards together", () => {
  test("hovering or focusing a pill highlights its card; clicking moves focus to the card", async () => {
    const user = userEvent.setup();
    const scroll = vi.spyOn(Element.prototype, "scrollIntoView");
    render(<Linked text="A [2] B" />);
    const pill = screen.getByRole("button", { name: "Source 2: kubernetes-pods.md" });
    await user.hover(pill);
    expect(screen.getByTestId("source-2").className).toMatch(/highlighted/);
    expect(scroll).toHaveBeenCalled();
    await user.unhover(pill);
    expect(screen.getByTestId("source-2").className).not.toMatch(/highlighted/);
    await user.click(pill);
    expect(screen.getByTestId("source-2").querySelector("button")).toHaveFocus();
    scroll.mockRestore();
  });

  test("the imperative handle exists", () => {
    const ref = createRef<SourceListHandle>();
    render(<SourceList ref={ref} sources={sources} highlighted={undefined} />);
    expect(typeof ref.current?.reveal).toBe("function");
  });
});
