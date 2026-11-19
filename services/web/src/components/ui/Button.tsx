import type { ButtonHTMLAttributes } from "react";
import { Spinner } from "./Spinner";
import styles from "./Button.module.css";

type Props = ButtonHTMLAttributes<HTMLButtonElement> & {
  variant?: "primary" | "secondary" | "ghost";
  busy?: boolean;
};

export function Button({ variant = "secondary", busy = false, className, type = "button", children, ...rest }: Props) {
  const classes = [styles.button, styles[variant], className].filter(Boolean).join(" ");
  return (
    <button type={type} className={classes} aria-busy={busy || undefined} {...rest}>
      {busy && <Spinner />}
      {children}
    </button>
  );
}
