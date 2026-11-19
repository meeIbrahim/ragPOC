import { forwardRef, useState, type KeyboardEvent } from "react";
import { MAX_QUERY_CHARS, QUERY_COUNTER_FROM } from "../../limits";
import { questionLength, validateQuestion } from "../../hooks/useAsk";
import { Button } from "../ui/Button";
import { InlineError } from "../ui/InlineError";
import styles from "./QuestionForm.module.css";

export const QUESTION_ERROR_ID = "question-error";

interface Props {
  value: string;
  onChange: (v: string) => void;
  /** Called only with a valid question. */
  onSubmit: () => void;
  onStop: () => void;
  /** Sending or streaming: the button reads Stop and the textarea is readonly. */
  busy: boolean;
  /** Set when asking isn't possible yet; shown as the helper text. */
  disabledReason: string | undefined;
  /** The server's 400 about the question, verbatim. */
  serverError: string | undefined;
  buttonRef?: React.Ref<HTMLButtonElement>;
}

const count = (n: number) => n.toLocaleString("en-US");

export const QuestionForm = forwardRef<HTMLTextAreaElement, Props>(function QuestionForm(
  { value, onChange, onSubmit, onStop, busy, disabledReason, serverError, buttonRef },
  ref,
) {
  const [clientError, setClientError] = useState<string | undefined>(undefined);
  const length = questionLength(value);
  const tooLong = length > MAX_QUERY_CHARS;
  const disabled = disabledReason !== undefined;
  const error = clientError ?? serverError;

  const submit = () => {
    if (busy || disabled) {
      return;
    }
    const problem = validateQuestion(value);
    setClientError(problem);
    if (!problem) {
      onSubmit();
    }
  };

  const onKeyDown = (e: KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === "Enter" && !e.shiftKey && !e.nativeEvent.isComposing) {
      e.preventDefault();
      submit();
    }
  };

  return (
    <form
      className={styles.form}
      onSubmit={(e) => {
        e.preventDefault();
        submit();
      }}
    >
      <label htmlFor="question" className={styles.label}>
        Question
      </label>
      <textarea
        id="question"
        ref={ref}
        className={styles.input}
        rows={3}
        placeholder="Ask a question about your documents"
        value={value}
        readOnly={busy}
        disabled={disabled}
        aria-invalid={error ? true : undefined}
        aria-describedby={error ? QUESTION_ERROR_ID : undefined}
        onChange={(e) => {
          setClientError(undefined);
          onChange(e.target.value);
        }}
        onKeyDown={onKeyDown}
      />
      {error && <InlineError id={QUESTION_ERROR_ID}>{error}</InlineError>}
      <div className={styles.footer}>
        <span className={styles.helper}>{disabledReason ?? "Enter to send · Shift+Enter for a new line"}</span>
        {length > QUERY_COUNTER_FROM && (
          <span className={tooLong ? `${styles.counter} ${styles.over}` : styles.counter} data-testid="counter">
            {count(length)} / {count(MAX_QUERY_CHARS)}
          </span>
        )}
        <Button
          ref={buttonRef}
          variant={busy ? "secondary" : "primary"}
          aria-label={busy ? "Stop" : "Ask"}
          disabled={!busy && (disabled || tooLong)}
          onClick={busy ? onStop : submit}
        >
          {busy ? "Stop" : "Ask"}
        </Button>
      </div>
    </form>
  );
});
