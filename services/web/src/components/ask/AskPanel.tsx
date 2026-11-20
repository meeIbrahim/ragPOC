import { useEffect, useRef, useState, type KeyboardEvent } from "react";
import { useAsk, type AskStreamFn } from "../../hooks/useAsk";
import { useAnnounce } from "../ui/LiveRegion";
import { AnswerArea, type DocsReadiness } from "./AnswerArea";
import { QuestionForm } from "./QuestionForm";
import { ScopeLine } from "./ScopeLine";
import styles from "./AskPanel.module.css";

interface Props {
  docs: DocsReadiness;
  selectedIds: string[];
  onClearSelection: () => void;
  /** The cap note from the documents panel (design 4). */
  capError: string | undefined;
  suggestions: string[];
  onRefreshDocuments: () => void;
  stream?: AskStreamFn;
}

const disabledReasons: Record<DocsReadiness["kind"], string | undefined> = {
  loading: "Loading documents…",
  failed: "Couldn't reach the server.",
  none: "Upload a document first.",
  noneIndexed: "Waiting for a document to finish indexing.",
  ready: undefined,
};

export function AskPanel({ docs, selectedIds, onClearSelection, capError, suggestions, onRefreshDocuments, stream }: Props) {
  const { state, busy, ask, retry, stop } = useAsk(stream);
  const announce = useAnnounce();
  const [question, setQuestion] = useState("");
  const textarea = useRef<HTMLTextAreaElement>(null);
  const button = useRef<HTMLButtonElement>(null);
  const started = useRef(false);

  useEffect(() => {
    switch (state.phase) {
      case "sending":
        started.current = false;
        announce("Searching documents");
        break;
      case "streaming":
        if (!started.current) {
          started.current = true;
          announce("Answer started");
        }
        break;
      case "done": {
        const n = state.sources.length;
        announce(`Answer ready, ${n} ${n === 1 ? "source" : "sources"}`);
        setQuestion("");
        // Back to the textarea only from the textarea or the button (design 5.2).
        const active = document.activeElement;
        if (active === textarea.current || active === button.current) {
          textarea.current?.focus();
        }
        break;
      }
      case "nothingRelevant":
        // Not Done: the question stays for rewording ("Try different wording").
        announce("No relevant information was found");
        break;
      case "stopped":
        announce("Stopped");
        setQuestion(state.question);
        break;
      case "error":
      case "invalid":
        setQuestion(state.question);
        break;
      default:
        break;
    }
    // Only phase changes matter here; state fields are read at that moment.
  }, [state.phase]);

  const onKeyDown = (e: KeyboardEvent<HTMLElement>) => {
    if (e.key === "Escape" && busy) {
      e.preventDefault();
      stop();
    }
  };

  const serverError = state.phase === "invalid" && state.target === "question" ? state.message : undefined;
  const selectionError = state.phase === "invalid" && state.target === "selection" ? state.message : undefined;

  return (
    <section className={styles.panel} aria-labelledby="ask-title" onKeyDown={onKeyDown}>
      <h2 id="ask-title" className={styles.heading}>
        Ask
      </h2>
      <ScopeLine count={selectedIds.length} onClear={onClearSelection} error={capError ?? selectionError} />
      <AnswerArea
        state={state}
        docs={docs}
        suggestions={suggestions}
        onSuggestion={(s) => {
          setQuestion(s);
          textarea.current?.focus();
        }}
        onRetry={retry}
        onRefreshDocuments={onRefreshDocuments}
      />
      <QuestionForm
        ref={textarea}
        buttonRef={button}
        value={question}
        onChange={setQuestion}
        onSubmit={() => ask(question, selectedIds)}
        onStop={stop}
        busy={busy}
        disabledReason={disabledReasons[docs.kind]}
        serverError={serverError}
      />
    </section>
  );
}
