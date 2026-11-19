import { useCallback, useEffect, useRef, useState } from "react";
import { listDocuments } from "../api/http";
import type { DocumentEntry } from "../api/types";
import { POLL_INTERVAL_MS, POLL_STALE_AFTER } from "../limits";

/** The first load: drives the page's skeleton and "Couldn't reach the server" states. */
export type LoadState = "loading" | "ready" | "failed";
/** Later polls: "Reconnecting…" after one failure, "Can't reach the server" after three (design 3.5). */
export type PollState = "ok" | "reconnecting" | "stale";

interface State {
  entries: DocumentEntry[];
  truncated: boolean;
  load: LoadState;
  poll: PollState;
  failures: number;
  /** Successful fetches so far; lets callers act once per poll. */
  version: number;
}

const initial: State = { entries: [], truncated: false, load: "loading", poll: "ok", failures: 0, version: 0 };

function isVisible() {
  return document.visibilityState !== "hidden";
}

/**
 * GET /documents on mount, then every POLL_INTERVAL_MS while `active` and the tab is visible.
 * A failed poll keeps the current entries. Becoming visible fetches at once.
 */
export function useDocuments(active: boolean) {
  const [state, setState] = useState<State>(initial);
  const [visible, setVisible] = useState(isVisible);
  const inFlight = useRef<AbortController | null>(null);

  const fetchNow = useCallback(async () => {
    if (inFlight.current) {
      return;
    }
    const ctrl = new AbortController();
    inFlight.current = ctrl;
    try {
      const list = await listDocuments(ctrl.signal);
      if (ctrl.signal.aborted) {
        return;
      }
      setState((s) => ({
        entries: list.documents,
        truncated: list.truncated,
        load: "ready",
        poll: "ok",
        failures: 0,
        version: s.version + 1,
      }));
    } catch {
      if (ctrl.signal.aborted) {
        return;
      }
      setState((s) => {
        const failures = s.failures + 1;
        return {
          ...s,
          failures,
          load: s.load === "ready" ? "ready" : "failed",
          poll: failures >= POLL_STALE_AFTER ? "stale" : "reconnecting",
        };
      });
    } finally {
      if (inFlight.current === ctrl) {
        inFlight.current = null;
      }
    }
  }, []);

  const refresh = useCallback(() => {
    setState((s) => (s.load === "failed" ? { ...s, load: "loading" } : s));
    void fetchNow();
  }, [fetchNow]);

  useEffect(() => {
    void fetchNow();
    return () => {
      inFlight.current?.abort();
      inFlight.current = null;
    };
  }, [fetchNow]);

  useEffect(() => {
    const onChange = () => {
      const v = isVisible();
      setVisible(v);
      if (v) {
        void fetchNow();
      }
    };
    document.addEventListener("visibilitychange", onChange);
    return () => document.removeEventListener("visibilitychange", onChange);
  }, [fetchNow]);

  useEffect(() => {
    if (!active || !visible) {
      return undefined;
    }
    const id = setInterval(() => void fetchNow(), POLL_INTERVAL_MS);
    return () => clearInterval(id);
  }, [active, visible, fetchNow]);

  return {
    entries: state.entries,
    truncated: state.truncated,
    load: state.load,
    poll: state.poll,
    version: state.version,
    refresh,
  };
}
