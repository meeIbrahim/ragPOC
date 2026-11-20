import type { ReactNode } from "react";
import { ErrorIcon } from "./icons";
import styles from "./InlineError.module.css";

/** Field or panel error; give it an id and point the control's aria-describedby at it. */
export function InlineError({ id, children }: { id?: string; children: ReactNode }) {
  return (
    <p id={id} role="alert" className={styles.error}>
      <ErrorIcon />
      <span>{children}</span>
    </p>
  );
}
