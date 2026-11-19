export type Segment = { type: "text"; text: string } | { type: "cite"; n: number };

/**
 * Splits answer text into text and [n] citation segments. Always called on the whole
 * accumulated answer, never per delta, so a marker split across deltas becomes one pill (T4).
 * A number outside 1..sourceCount stays text (design 5.3).
 */
export function splitCitations(text: string, sourceCount: number): Segment[] {
  const out: Segment[] = [];
  let last = 0;
  const pushText = (t: string) => {
    if (!t) {
      return;
    }
    const prev = out.at(-1);
    if (prev?.type === "text") {
      prev.text += t;
    } else {
      out.push({ type: "text", text: t });
    }
  };
  for (const m of text.matchAll(/\[(\d{1,3})\]/g)) {
    const n = Number(m[1]);
    if (n < 1 || n > sourceCount) {
      continue;
    }
    pushText(text.slice(last, m.index));
    out.push({ type: "cite", n });
    last = m.index + m[0].length;
  }
  pushText(text.slice(last));
  return out;
}
