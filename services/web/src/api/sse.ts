export interface SseEvent {
  event: string;
  data: string;
}

/**
 * Incremental text/event-stream parser (spec A7). Feed decoded text with push(); a blank line
 * ends an event, multi-line data is joined with "\n". Lines are cut only at "\n", so a CRLF split
 * across two reads is still one line ending.
 */
export class SseParser {
  private buffer = "";
  private name = "";
  private data: string[] = [];

  push(chunk: string): SseEvent[] {
    this.buffer += chunk;
    const out: SseEvent[] = [];
    let nl = this.buffer.indexOf("\n");
    while (nl >= 0) {
      let line = this.buffer.slice(0, nl);
      this.buffer = this.buffer.slice(nl + 1);
      if (line.endsWith("\r")) {
        line = line.slice(0, -1);
      }
      const e = this.line(line);
      if (e) {
        out.push(e);
      }
      nl = this.buffer.indexOf("\n");
    }
    return out;
  }

  /** Call at end of stream. True when a partial event was left undispatched. */
  end(): boolean {
    return this.buffer !== "" || this.name !== "" || this.data.length > 0;
  }

  private line(line: string): SseEvent | undefined {
    if (line === "") {
      const e = this.data.length > 0 ? { event: this.name || "message", data: this.data.join("\n") } : undefined;
      this.name = "";
      this.data = [];
      return e;
    }
    if (line.startsWith(":")) {
      return undefined;
    }
    const colon = line.indexOf(":");
    const field = colon < 0 ? line : line.slice(0, colon);
    let value = colon < 0 ? "" : line.slice(colon + 1);
    if (value.startsWith(" ")) {
      value = value.slice(1);
    }
    if (field === "event") {
      this.name = value;
    } else if (field === "data") {
      this.data.push(value);
    }
    return undefined;
  }
}
