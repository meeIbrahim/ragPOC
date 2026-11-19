import { forwardRef, useState } from "react";
import type { Source } from "../../api/types";
import { ChevronIcon } from "../ui/icons";
import styles from "./SourceCard.module.css";

interface Props {
  n: number;
  source: Source;
  highlighted: boolean;
  index: number;
}

/** One compact line; click or Enter shows the snippet (design 5.3). */
export const SourceCard = forwardRef<HTMLButtonElement, Props>(function SourceCard({ n, source, highlighted, index }, ref) {
  const [open, setOpen] = useState(false);
  const classes = [styles.card, n === 1 && styles.first, highlighted && styles.highlighted].filter(Boolean).join(" ");
  return (
    <li className={classes} style={{ animationDelay: `calc(var(--duration-stagger) * ${index})` }} data-testid={`source-${n}`}>
      <button ref={ref} type="button" className={styles.row} aria-expanded={open} onClick={() => setOpen((o) => !o)}>
        <span className={styles.pill}>[{n}]</span>
        <span className={styles.name} dir="auto" title={source.filename}>
          {source.filename}
        </span>
        <span className={styles.score} title="relevance">
          {source.score.toFixed(2)}
        </span>
        <ChevronIcon open={open} />
      </button>
      {open && (
        <p className={styles.snippet} dir="auto">
          {source.snippet}
        </p>
      )}
    </li>
  );
});
