import type { DocumentEntry } from "../api/types";

/** "kubernetes-pods.md" reads "kubernetes pods". */
export function readableName(filename: string): string {
  const dot = filename.lastIndexOf(".");
  const base = dot > 0 ? filename.slice(0, dot) : filename;
  return base.replace(/[-_]+/g, " ").replace(/\s+/g, " ").trim();
}

/** Up to 3 chips from the newest indexed files (design 5.2). Compare needs two files. */
export function suggestions(entries: DocumentEntry[]): string[] {
  const names = entries
    .filter((e) => e.status === "INDEXED")
    .sort((a, b) => b.createdAt - a.createdAt)
    .map((e) => readableName(e.filename));
  const [first, second] = names;
  if (first === undefined) {
    return [];
  }
  if (second === undefined) {
    return [`Summarize ${first}`, `Key points of ${first}`];
  }
  return [`Summarize ${first}`, `Key points of ${second}`, `Compare ${first} and ${second}`];
}
