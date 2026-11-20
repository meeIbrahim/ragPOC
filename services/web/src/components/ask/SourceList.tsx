import { forwardRef, useImperativeHandle, useRef } from "react";
import type { Source } from "../../api/types";
import { SourceCard } from "./SourceCard";
import styles from "./SourceList.module.css";

export interface SourceListHandle {
  /** Scrolls card n into view (pill hover) and optionally focuses it (pill click). */
  reveal: (n: number, focus: boolean) => void;
}

export const SourceList = forwardRef<SourceListHandle, { sources: Source[]; highlighted: number | undefined }>(
  function SourceList({ sources, highlighted }, ref) {
    const cards = useRef<Array<HTMLButtonElement | null>>([]);
    useImperativeHandle(ref, () => ({
      reveal(n, focus) {
        const el = cards.current[n - 1];
        el?.scrollIntoView({ block: "nearest" });
        if (focus) {
          el?.focus();
        }
      },
    }));
    return (
      <section aria-labelledby="sources-title" className={styles.section}>
        <h2 id="sources-title" className={styles.heading}>
          Sources
        </h2>
        <ol className={styles.list}>
          {sources.map((s, i) => (
            <SourceCard
              key={i}
              ref={(el) => {
                cards.current[i] = el;
              }}
              n={i + 1}
              index={i}
              source={s}
              highlighted={highlighted === i + 1}
            />
          ))}
        </ol>
      </section>
    );
  },
);
