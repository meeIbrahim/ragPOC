import { act, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, test, vi } from "vitest";
import { TOAST_TIMEOUT_MS } from "../../limits";
import { Button } from "./Button";
import { InlineError } from "./InlineError";
import { LiveRegionProvider, useAnnounce } from "./LiveRegion";
import { Skeleton } from "./Skeleton";
import { StatusBadge } from "./StatusBadge";
import { ToastProvider, useToast } from "./Toast";

afterEach(() => {
  vi.useRealTimers();
});

describe("Button", () => {
  test("defaults to type=button and keeps its label while busy", () => {
    render(
      <Button variant="primary" busy>
        Upload file
      </Button>,
    );
    const b = screen.getByRole("button", { name: "Upload file" });
    expect(b).toHaveAttribute("type", "button");
    expect(b).toHaveAttribute("aria-busy", "true");
  });
});

describe("StatusBadge", () => {
  test.each([
    ["queued", "Queued"],
    ["uploading", "Uploading"],
    ["processing", "Processing"],
    ["indexed", "Indexed"],
    ["failed", "Failed"],
  ] as const)("%s shows an icon and the text %s, never color alone", (status, label) => {
    const { container } = render(<StatusBadge status={status} />);
    expect(screen.getByText(label)).toBeInTheDocument();
    const icon = container.querySelector("svg");
    expect(icon, "every badge has an icon").not.toBeNull();
    expect(icon).toHaveAttribute("aria-hidden", "true");
  });
});

describe("Skeleton", () => {
  test("is hidden from assistive tech", () => {
    const { container } = render(<Skeleton />);
    expect(container.firstChild).toHaveAttribute("aria-hidden", "true");
  });
});

describe("InlineError", () => {
  test("is an alert with an id for aria-describedby", () => {
    render(<InlineError id="q-err">Question is empty.</InlineError>);
    expect(screen.getByRole("alert")).toHaveAttribute("id", "q-err");
    expect(screen.getByRole("alert")).toHaveTextContent("Question is empty.");
  });
});

function ToastButtons() {
  const toast = useToast();
  return (
    <>
      <button onClick={() => toast({ kind: "success", message: "copied" })}>ok</button>
      <button onClick={() => toast({ kind: "error", message: "it failed" })}>bad</button>
    </>
  );
}

describe("Toast", () => {
  test(`a non-error toast is a status and hides after ${TOAST_TIMEOUT_MS} ms`, async () => {
    vi.useFakeTimers();
    const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime });
    render(
      <ToastProvider>
        <ToastButtons />
      </ToastProvider>,
    );
    await user.click(screen.getByRole("button", { name: "ok" }));
    expect(screen.getByRole("status")).toHaveTextContent("copied");
    await act(async () => {
      await vi.advanceTimersByTimeAsync(TOAST_TIMEOUT_MS - 1);
    });
    expect(screen.queryByText("copied")).toBeInTheDocument();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(1);
    });
    expect(screen.queryByText("copied")).not.toBeInTheDocument();
  });

  test("an error toast is an alert and stays until dismissed", async () => {
    vi.useFakeTimers();
    const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime });
    render(
      <ToastProvider>
        <ToastButtons />
      </ToastProvider>,
    );
    await user.click(screen.getByRole("button", { name: "bad" }));
    await act(async () => {
      await vi.advanceTimersByTimeAsync(TOAST_TIMEOUT_MS * 3);
    });
    expect(screen.getByRole("alert")).toHaveTextContent("it failed");
    await user.click(screen.getByRole("button", { name: "Dismiss" }));
    expect(screen.queryByText("it failed")).not.toBeInTheDocument();
  });
});

function Announcer({ text }: { text: string }) {
  const announce = useAnnounce();
  return <button onClick={() => announce(text)}>say</button>;
}

describe("LiveRegion", () => {
  test("one polite status region announces the text, again when repeated", async () => {
    const user = userEvent.setup();
    render(
      <LiveRegionProvider>
        <Announcer text="Answer ready, 3 sources" />
      </LiveRegionProvider>,
    );
    const region = screen.getByRole("status");
    expect(region).toHaveAttribute("aria-live", "polite");
    await user.click(screen.getByRole("button", { name: "say" }));
    expect(region).toHaveTextContent("Answer ready, 3 sources");
    const first = region.firstElementChild;
    await user.click(screen.getByRole("button", { name: "say" }));
    expect(region.firstElementChild, "a repeat must replace the node so it is read again").not.toBe(first);
  });
});
