import { useEffect, useRef, useState } from "react";
import type { AskState } from "../../hooks/useAsk";
import { formatSeconds } from "../../lib/time";
import { Button } from "../ui/Button";
import { ErrorIcon, InfoIcon, SpinnerIcon } from "../ui/icons";
import { useToast } from "../ui/Toast";
import { AnswerText } from "./AnswerText";
import { SourceList, type SourceListHandle } from "./SourceList";
import styles from "./AnswerArea.module.css";

/** What the documents allow before any question (design 5.2 first rows, page states). */
export type DocsReadiness =
  | { kind: "loading" }
  | { kind: "failed" }
  | { kind: "none" }
  | { kind: "noneIndexed"; processing: number }
  | { kind: "ready" };

interface Props {
  state: AskState;
  docs: DocsReadiness;
  suggestions: string[];
  onSuggestion: (text: string) => void;
  onRetry: () => void;
  onRefreshDocuments: () => void;
}

// Following stops once the reader is more than this far from the bottom (design 2).
const FOLLOW_SLACK = 48;

const errorTitle = {
  unavailable: "The answer service is unavailable.",
  other: "Something went wrong while answering.",
  connectionLost: "The connection was lost.",
} as const;

export function AnswerArea({ state, docs, suggestions, onSuggestion, onRetry, onRefreshDocuments }: Props) {
  const toast = useToast();
  const scroller = useRef<HTMLDivElement>(null);
  const list = useRef<SourceListHandle>(null);
  const tryAgain = useRef<HTMLButtonElement>(null);
  const [highlight, setHighlight] = useState<number | undefined>(undefined);
  const [following, setFollowing] = useState(true);

  const answer = "answer" in state ? state.answer : "";
  const sources = "sources" in state ? state.sources : [];
  const streaming = state.phase === "streaming";

  useEffect(() => {
    if (state.phase === "sending") {
      setFollowing(true);
      setHighlight(undefined);
    }
    if (state.phase === "error") {
      tryAgain.current?.focus();
    }
  }, [state.phase]);

  useEffect(() => {
    const el = scroller.current;
    if (el && following && (streaming || state.phase === "sources")) {
      el.scrollTop = el.scrollHeight;
    }
  }, [answer, following, streaming, state.phase]);

  const onScroll = () => {
    const el = scroller.current;
    if (el) {
      setFollowing(el.scrollHeight - el.scrollTop - el.clientHeight <= FOLLOW_SLACK);
    }
  };

  const jump = () => {
    const el = scroller.current;
    if (el) {
      el.scrollTop = el.scrollHeight;
    }
    setFollowing(true);
  };

  const copy = async () => {
    try {
      await navigator.clipboard.writeText(answer);
      toast({ kind: "success", message: "Answer copied." });
    } catch {
      toast({ kind: "error", message: "Couldn't copy the answer." });
    }
  };

  let content;
  if (state.phase === "idle" || state.phase === "invalid") {
    content = <Empty docs={docs} suggestions={suggestions} onSuggestion={onSuggestion} onRefresh={onRefreshDocuments} />;
  } else {
    content = (
      <div className={styles.turn} key={state.question}>
        <p className={styles.question} dir="auto">
          {state.question}
        </p>
        {state.phase === "sending" && (
          <p className={styles.progress}>
            <SpinnerIcon /> Searching documents…
          </p>
        )}
        {state.phase === "nothingRelevant" && (
          <div className={styles.neutral}>
            <InfoIcon />
            <div>
              <p>No relevant information was found in the indexed documents.</p>
              <p className={styles.hint}>
                {state.hadSelection ? "Try different wording, or clear the selection." : "Try different wording, or select other documents."}
              </p>
            </div>
          </div>
        )}
        {sources.length > 0 && <SourceList ref={list} sources={sources} highlighted={highlight} />}
        {state.phase === "sources" && (
          <p className={styles.progress}>
            <SpinnerIcon /> Writing answer…
          </p>
        )}
        {(answer || streaming) && (
          <section aria-label="Answer" className={styles.answerBlock}>
            <h2 className={styles.heading}>Answer</h2>
            <AnswerText
              text={answer}
              sourceNames={sources.map((s) => s.filename)}
              streaming={streaming}
              onHighlight={(n) => {
                setHighlight(n);
                if (n) {
                  list.current?.reveal(n, false);
                }
              }}
              onPillClick={(n) => list.current?.reveal(n, true)}
            />
          </section>
        )}
        {state.phase === "done" && (
          <div className={styles.meta}>
            <span>
              Answered from {sources.length} {sources.length === 1 ? "source" : "sources"} · {formatSeconds(state.elapsedMs)}
            </span>
            <Button variant="ghost" onClick={() => void copy()}>
              Copy answer
            </Button>
          </div>
        )}
        {state.phase === "stopped" && <p className={styles.meta}>Stopped.</p>}
        {state.phase === "error" && (
          <div className={styles.errorCard} role="alert">
            <ErrorIcon />
            <div className={styles.errorBody}>
              <p className={styles.errorTitle}>{errorTitle[state.kind]}</p>
              {state.kind === "unavailable" && <p className={styles.hint}>Try again in a moment.</p>}
              {state.kind === "other" && state.message && <p className={styles.hint}>{state.message}</p>}
              <Button ref={tryAgain} variant="secondary" onClick={onRetry}>
                Try again
              </Button>
            </div>
          </div>
        )}
      </div>
    );
  }

  return (
    <div className={styles.area}>
      <div ref={scroller} className={styles.scroller} onScroll={onScroll} data-testid="answer-scroller">
        {content}
      </div>
      {streaming && !following && (
        <div className={styles.jump}>
          <Button variant="ghost" onClick={jump}>
            Jump to latest
          </Button>
        </div>
      )}
    </div>
  );
}

function Empty({
  docs,
  suggestions,
  onSuggestion,
  onRefresh,
}: {
  docs: DocsReadiness;
  suggestions: string[];
  onSuggestion: (t: string) => void;
  onRefresh: () => void;
}) {
  switch (docs.kind) {
    case "loading":
      return null;
    case "failed":
      return (
        <div className={styles.center}>
          <p>Couldn't reach the server.</p>
          <Button variant="secondary" onClick={onRefresh}>
            Try again
          </Button>
        </div>
      );
    case "none":
      return (
        <div className={styles.center}>
          <p>Upload a document to start asking questions.</p>
        </div>
      );
    case "noneIndexed":
      return (
        <div className={styles.center}>
          <p>Your documents are still being indexed. You can ask once one is ready.</p>
          {docs.processing > 0 && <p className={styles.hint}>{docs.processing} processing</p>}
        </div>
      );
    default:
      return suggestions.length > 0 ? (
        <div className={styles.chips}>
          {suggestions.map((s) => (
            <Button key={s} variant="secondary" className={styles.chip} onClick={() => onSuggestion(s)}>
              {s}
            </Button>
          ))}
        </div>
      ) : null;
  }
}
