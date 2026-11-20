import { SLOW_PROCESSING_MS } from "../limits";

const SECOND = 1000;
const MINUTE = 60 * SECOND;
const HOUR = 60 * MINUTE;
const DAY = 24 * HOUR;

/** Processing time since created_at. A browser clock behind the server reads "0 s" (T6). */
export function formatElapsed(ms: number): string {
  const t = Math.max(0, ms);
  return t < MINUTE ? `${Math.floor(t / SECOND)} s` : `${Math.floor(t / MINUTE)} min`;
}

export function isSlow(ms: number): boolean {
  return ms >= SLOW_PROCESSING_MS;
}

export function formatRelative(msAgo: number): string {
  const t = Math.max(0, msAgo);
  if (t < MINUTE) {
    return "just now";
  }
  if (t < HOUR) {
    return `${Math.floor(t / MINUTE)} min ago`;
  }
  if (t < DAY) {
    return `${Math.floor(t / HOUR)} h ago`;
  }
  return `${Math.floor(t / DAY)} d ago`;
}

const absolute = new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "short" });

/** Local date and time for a title attribute; the API sends UTC. */
export function formatAbsolute(epochMs: number): string {
  return Number.isFinite(epochMs) ? absolute.format(epochMs) : "";
}

/** The Done line's answer time ("6.2 s"). */
export function formatSeconds(ms: number): string {
  return `${(Math.max(0, ms) / SECOND).toFixed(1)} s`;
}

const MB = 1_048_576;

/** Whole MB rounded up, so a file 1 byte over a 10 MB limit reads "11 MB" (design 3.1). */
export function formatMB(bytes: number): string {
  return `${Math.max(1, Math.ceil(bytes / MB))} MB`;
}
