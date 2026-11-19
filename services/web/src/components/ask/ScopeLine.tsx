import { Button } from "../ui/Button";
import { InlineError } from "../ui/InlineError";
import styles from "./ScopeLine.module.css";

export const SCOPE_ERROR_ID = "scope-error";

/** States what the next question searches (design 4). Amber never appears here. */
export function ScopeLine({ count, onClear, error }: { count: number; onClear: () => void; error: string | undefined }) {
  return (
    <div className={styles.scope}>
      <div className={styles.line}>
        <span>{count === 0 ? "Searching all indexed documents" : `Searching ${count} ${count === 1 ? "document" : "documents"}`}</span>
        {count > 0 && (
          <Button variant="ghost" onClick={onClear}>
            Clear
          </Button>
        )}
      </div>
      {error && <InlineError id={SCOPE_ERROR_ID}>{error}</InlineError>}
    </div>
  );
}
