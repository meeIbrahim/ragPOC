import { createContext, useCallback, useContext, useEffect, useRef, useState, type ReactNode } from "react";
import { TOAST_TIMEOUT_MS } from "../../limits";
import { Button } from "./Button";
import { CheckIcon, ErrorIcon, InfoIcon } from "./icons";
import styles from "./Toast.module.css";

export interface ToastInput {
  kind: "success" | "info" | "error";
  message: ReactNode;
}

interface ToastItem extends ToastInput {
  id: number;
}

const ToastContext = createContext<(t: ToastInput) => void>(() => {});

/** Bottom-right toasts. Errors are alerts and stay until dismissed; others hide after --toast-timeout. */
export function ToastProvider({ children }: { children: ReactNode }) {
  const [items, setItems] = useState<ToastItem[]>([]);
  const next = useRef(0);
  const timers = useRef(new Map<number, ReturnType<typeof setTimeout>>());

  const dismiss = useCallback((id: number) => {
    const t = timers.current.get(id);
    if (t) {
      clearTimeout(t);
      timers.current.delete(id);
    }
    setItems((list) => list.filter((i) => i.id !== id));
  }, []);

  const toast = useCallback(
    (input: ToastInput) => {
      next.current += 1;
      const id = next.current;
      setItems((list) => [...list, { ...input, id }]);
      if (input.kind !== "error") {
        timers.current.set(
          id,
          setTimeout(() => dismiss(id), TOAST_TIMEOUT_MS),
        );
      }
    },
    [dismiss],
  );

  useEffect(() => {
    const map = timers.current;
    return () => {
      map.forEach(clearTimeout);
      map.clear();
    };
  }, []);

  return (
    <ToastContext.Provider value={toast}>
      {children}
      <div className={styles.stack}>
        {items.map((t) => (
          <div key={t.id} role={t.kind === "error" ? "alert" : "status"} className={`${styles.toast} ${styles[t.kind]}`}>
            {t.kind === "error" ? <ErrorIcon /> : t.kind === "success" ? <CheckIcon /> : <InfoIcon />}
            <span className={styles.message}>{t.message}</span>
            {t.kind === "error" && (
              <Button variant="ghost" onClick={() => dismiss(t.id)}>
                Dismiss
              </Button>
            )}
          </div>
        ))}
      </div>
    </ToastContext.Provider>
  );
}

export function useToast() {
  return useContext(ToastContext);
}
