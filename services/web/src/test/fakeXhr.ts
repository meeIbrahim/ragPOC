import { vi } from "vitest";

type Listener = (() => void) | null;

/** A controllable XMLHttpRequest. Tests drive progress and completion through the instance. */
export class FakeXhr {
  static instances: FakeXhr[] = [];

  method = "";
  url = "";
  body: unknown = undefined;
  status = 0;
  headers: Record<string, string> = {};
  aborted = false;
  upload: { onprogress: ((e: { loaded: number; total: number; lengthComputable: boolean }) => void) | null } = {
    onprogress: null,
  };
  onload: Listener = null;
  onerror: Listener = null;
  onabort: Listener = null;

  constructor() {
    FakeXhr.instances.push(this);
  }

  open(method: string, url: string) {
    this.method = method;
    this.url = url;
  }

  setRequestHeader(name: string, value: string) {
    this.headers[name] = value;
  }

  send(body: unknown) {
    this.body = body;
  }

  abort() {
    this.aborted = true;
    this.onabort?.();
  }

  progress(loaded: number, total: number) {
    this.upload.onprogress?.({ loaded, total, lengthComputable: true });
  }

  respond(status: number) {
    this.status = status;
    this.onload?.();
  }

  fail() {
    this.onerror?.();
  }

  static latest(): FakeXhr {
    const x = FakeXhr.instances.at(-1);
    if (!x) {
      throw new Error("no XMLHttpRequest was created");
    }
    return x;
  }
}

export function installFakeXhr() {
  FakeXhr.instances = [];
  vi.stubGlobal("XMLHttpRequest", FakeXhr);
}
