import { useCallback, useEffect, useRef, useState } from "react";
import { askStream, type AskEvent } from "../api/ask";
import type { Source } from "../api/types";
import { MAX_QUERY_CHARS, MAX_SOURCES_SHOWN } from "../limits";

export type AskStreamFn = (question: string, uploadIds: string[], signal: AbortSignal) => AsyncIterable<AskEvent>;

export type ErrorKind = "unavailable" | "other" | "connectionLost";

/** One question and its answer (single-turn, PRD L9). Every phase after idle keeps the question. */
export type AskState =
  | { phase: "idle" }
  | { phase: "sending"; question: string }
  | { phase: "sources"; question: string; sources: Source[] }
  | { phase: "streaming"; question: string; sources: Source[]; answer: string }
  | { phase: "done"; question: string; sources: Source[]; answer: string; elapsedMs: number }
  | { phase: "nothingRelevant"; question: string; hadSelection: boolean }
  | { phase: "stopped"; question: string; sources: Source[]; answer: string }
  | { phase: "error"; kind: ErrorKind; message: string; question: string; sources: Source[]; answer: string }
  /** The server's 400, shown verbatim under the question or the scope line. */
  | { phase: "invalid"; message: string; target: "question" | "selection"; question: string };

/** Code points, like the server's utf8.RuneCountInString; whitespace-only is empty. */
export function questionLength(q: string): number {
  return [...q].length;
}

export function validateQuestion(q: string): string | undefined {
  if (q.trim() === "") {
    return "Question is empty.";
  }
  if (questionLength(q) > MAX_QUERY_CHARS) {
    return `Question is longer than ${MAX_QUERY_CHARS} characters.`;
  }
  return undefined;
}

export function useAsk(stream: AskStreamFn = askStream) {
  const [state, setState] = useState<AskState>({ phase: "idle" });
  const current = useRef<AbortController | null>(null);
  const last = useRef<{ question: string; ids: string[] } | null>(null);

  const run = useCallback(
    async (question: string, ids: string[]) => {
      current.current?.abort();
      const ctrl = new AbortController();
      current.current = ctrl;
      last.current = { question, ids };
      const started = Date.now();
      // Only the newest request may change state; a stale or unmounted one is ignored.
      const set = (s: AskState) => {
        if (current.current === ctrl) {
          setState(s);
        }
      };
      set({ phase: "sending", question });

      let sources: Source[] = [];
      let answer = "";
      let nothing = false;
      try {
        for await (const e of stream(question, ids, ctrl.signal)) {
          if (current.current !== ctrl) {
            return;
          }
          switch (e.type) {
            case "sources":
              sources = e.sources.slice(0, MAX_SOURCES_SHOWN);
              nothing = sources.length === 0;
              set(nothing ? { phase: "nothingRelevant", question, hadSelection: ids.length > 0 } : { phase: "sources", question, sources });
              break;
            case "delta":
              if (!nothing) {
                answer += e.text;
                set({ phase: "streaming", question, sources, answer });
              }
              break;
            case "done":
              if (!nothing) {
                set({ phase: "done", question, sources, answer, elapsedMs: Date.now() - started });
              }
              break;
            case "error":
              set({
                phase: "error",
                kind: e.code === "UNAVAILABLE" ? "unavailable" : "other",
                message: e.message,
                question,
                sources,
                answer,
              });
              break;
            case "failed":
              set({ phase: "error", kind: "other", message: e.message, question, sources, answer });
              break;
            case "connectionLost":
              set({ phase: "error", kind: "connectionLost", message: "", question, sources, answer });
              break;
            case "invalid":
              set({ phase: "invalid", message: e.message, target: e.message.includes("upload_id") ? "selection" : "question", question });
              break;
            case "stopped":
              set({ phase: "stopped", question, sources, answer });
              break;
          }
        }
      } catch {
        // askStream turns every outcome into an event; anything thrown here is a broken stream.
        set({ phase: "error", kind: "connectionLost", message: "", question, sources, answer });
      }
      if (current.current === ctrl) {
        current.current = null;
      }
    },
    [stream],
  );

  const ask = useCallback((question: string, ids: string[]) => void run(question, ids), [run]);

  const retry = useCallback(() => {
    if (last.current) {
      void run(last.current.question, last.current.ids);
    }
  }, [run]);

  const stop = useCallback(() => {
    current.current?.abort();
  }, []);

  useEffect(
    () => () => {
      const ctrl = current.current;
      current.current = null;
      ctrl?.abort();
    },
    [],
  );

  const busy = state.phase === "sending" || state.phase === "sources" || state.phase === "streaming";
  return { state, busy, ask, retry, stop };
}
