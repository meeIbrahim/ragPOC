import { fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { describe, expect, test, vi } from "vitest";
import { QuestionForm } from "./QuestionForm";
import { ScopeLine } from "./ScopeLine";

describe("ScopeLine (design 4)", () => {
  test("none selected searches all; n selected offers Clear", async () => {
    const onClear = vi.fn();
    const { rerender } = render(<ScopeLine count={0} onClear={onClear} error={undefined} />);
    expect(screen.getByText("Searching all indexed documents")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Clear" })).not.toBeInTheDocument();
    rerender(<ScopeLine count={1} onClear={onClear} error={undefined} />);
    expect(screen.getByText("Searching 1 document")).toBeInTheDocument();
    rerender(<ScopeLine count={2} onClear={onClear} error={undefined} />);
    expect(screen.getByText("Searching 2 documents")).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Clear" }));
    expect(onClear).toHaveBeenCalled();
  });

  test("the cap or selection error shows as an alert", () => {
    render(<ScopeLine count={100} onClear={() => {}} error="You can select up to 100 documents." />);
    expect(screen.getByRole("alert")).toHaveTextContent("You can select up to 100 documents.");
  });
});

type FormProps = Partial<Parameters<typeof QuestionForm>[0]>;

function Form(props: FormProps & { initial?: string }) {
  const [value, setValue] = useState(props.initial ?? "");
  return (
    <QuestionForm
      value={value}
      onChange={setValue}
      onSubmit={props.onSubmit ?? (() => {})}
      onStop={props.onStop ?? (() => {})}
      busy={props.busy ?? false}
      disabledReason={props.disabledReason}
      serverError={props.serverError}
    />
  );
}

describe("QuestionForm (design 5.1)", () => {
  test("a visible label, the placeholder and the helper line", () => {
    render(<Form />);
    const box = screen.getByLabelText("Question");
    expect(box).toHaveAttribute("placeholder", "Ask a question about your documents");
    expect(screen.getByText("Enter to send · Shift+Enter for a new line")).toBeInTheDocument();
  });

  test("Enter sends, Shift+Enter adds a new line", async () => {
    const onSubmit = vi.fn();
    render(<Form onSubmit={onSubmit} />);
    const box = screen.getByLabelText("Question");
    await userEvent.type(box, "line one{Shift>}{Enter}{/Shift}line two");
    expect(box).toHaveValue("line one\nline two");
    expect(onSubmit).not.toHaveBeenCalled();
    await userEvent.type(box, "{Enter}");
    expect(onSubmit).toHaveBeenCalledTimes(1);
  });

  test("an empty question shows the inline error tied to the textarea and is not sent", async () => {
    const onSubmit = vi.fn();
    render(<Form onSubmit={onSubmit} initial="   " />);
    await userEvent.click(screen.getByRole("button", { name: "Ask" }));
    expect(onSubmit).not.toHaveBeenCalled();
    expect(screen.getByRole("alert")).toHaveTextContent("Question is empty.");
    expect(screen.getByLabelText("Question")).toHaveAttribute("aria-describedby", "question-error");
    await userEvent.type(screen.getByLabelText("Question"), "x");
    expect(screen.queryByRole("alert"), "typing clears the client error").not.toBeInTheDocument();
  });

  test("the server's 400 text shows verbatim", () => {
    render(<Form serverError="question is longer than 2000 characters" />);
    expect(screen.getByRole("alert")).toHaveTextContent("question is longer than 2000 characters");
  });

  test("the counter appears past 1800 code points and disables Ask past 2000", () => {
    const { unmount } = render(<Form initial={"a".repeat(1800)} />);
    expect(screen.queryByTestId("counter")).not.toBeInTheDocument();
    unmount();
    const r = render(<Form initial={"😀".repeat(1950)} />);
    expect(screen.getByTestId("counter")).toHaveTextContent("1,950 / 2,000");
    expect(screen.getByRole("button", { name: "Ask" })).toBeEnabled();
    r.unmount();
    render(<Form initial={"a".repeat(2001)} />);
    expect(screen.getByTestId("counter")).toHaveTextContent("2,001 / 2,000");
    expect(screen.getByTestId("counter").className).toMatch(/over/);
    expect(screen.getByRole("button", { name: "Ask" })).toBeDisabled();
  });

  test("busy: one button element reads Stop and stops; the textarea is readonly, not disabled", async () => {
    const onStop = vi.fn();
    const { rerender } = render(<Form />);
    const button = screen.getByRole("button", { name: "Ask" });
    rerender(<Form busy onStop={onStop} initial="q" />);
    const stop = screen.getByRole("button", { name: "Stop" });
    expect(stop, "the same element, so focus is never lost").toBe(button);
    expect(screen.getByLabelText("Question")).toHaveAttribute("readonly");
    expect(screen.getByLabelText("Question")).toBeEnabled();
    await userEvent.click(stop);
    expect(onStop).toHaveBeenCalled();
  });

  test("busy: Enter does not send again", () => {
    const onSubmit = vi.fn();
    render(<Form busy initial="q" onSubmit={onSubmit} />);
    fireEvent.keyDown(screen.getByLabelText("Question"), { key: "Enter" });
    expect(onSubmit).not.toHaveBeenCalled();
  });

  test("a disabled reason replaces the helper and disables the form", () => {
    render(<Form disabledReason="Loading documents…" />);
    expect(screen.getByText("Loading documents…")).toBeInTheDocument();
    expect(screen.getByLabelText("Question")).toBeDisabled();
    expect(screen.getByRole("button", { name: "Ask" })).toBeDisabled();
  });
});
