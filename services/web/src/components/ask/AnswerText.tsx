import { splitCitations } from "../../lib/citations";
import styles from "./AnswerText.module.css";

interface Props {
  /** The whole accumulated answer, never one delta (T4). */
  text: string;
  sourceNames: string[];
  streaming: boolean;
  onHighlight: (n: number | undefined) => void;
  onPillClick: (n: number) => void;
}

/**
 * Plain text with white-space: pre-wrap; markdown is shown literally (T3). [n] markers with a
 * matching source become amber pill buttons; anything else stays text. Never innerHTML.
 */
export function AnswerText({ text, sourceNames, streaming, onHighlight, onPillClick }: Props) {
  return (
    <div className={styles.answer} data-testid="answer-text">
      {splitCitations(text, sourceNames.length).map((seg, i) =>
        seg.type === "text" ? (
          <span key={i}>{seg.text}</span>
        ) : (
          <button
            key={i}
            type="button"
            className={styles.pill}
            aria-label={`Source ${seg.n}: ${sourceNames[seg.n - 1] ?? ""}`}
            onMouseEnter={() => onHighlight(seg.n)}
            onMouseLeave={() => onHighlight(undefined)}
            onFocus={() => onHighlight(seg.n)}
            onBlur={() => onHighlight(undefined)}
            onClick={() => onPillClick(seg.n)}
          >
            [{seg.n}]
          </button>
        ),
      )}
      {streaming && (
        <span className={styles.caret} aria-hidden="true">
          ▍
        </span>
      )}
    </div>
  );
}
