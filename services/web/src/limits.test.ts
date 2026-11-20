import { readFileSync } from "node:fs";
import { join } from "node:path";
import { expect, test } from "vitest";
import { ALLOWED_EXTENSIONS, MAX_FILE_BYTES, MAX_QUERY_CHARS, MAX_SELECTED } from "./limits";

// The browser can't read the server config, so limits.ts mirrors it (spec A6). This line-level
// read of the compose config fails when either side changes alone. The server stays the authority.
const config = readFileSync(join(__dirname, "..", "..", "..", "deploy", "config.compose.toml"), "utf8");

function key(table: string, name: string): string {
  const start = config.indexOf(`\n[${table}]\n`);
  expect(start, `[${table}] must exist in deploy/config.compose.toml`).toBeGreaterThanOrEqual(0);
  const body = config.slice(start + table.length + 4).split(/\n\[/)[0] ?? "";
  const line = body.split("\n").find((l) => l.startsWith(`${name}=`));
  expect(line, `${table}.${name} must exist in deploy/config.compose.toml`).toBeDefined();
  return (line as string).slice(name.length + 1).trim();
}

test("MAX_SELECTED mirrors [rag].max_document_ids", () => {
  expect(MAX_SELECTED).toBe(Number(key("rag", "max_document_ids")));
});

test("MAX_QUERY_CHARS mirrors [rag].max_query_chars", () => {
  expect(MAX_QUERY_CHARS).toBe(Number(key("rag", "max_query_chars")));
});

test("MAX_FILE_BYTES mirrors [rag.consumer].max_file_bytes", () => {
  expect(MAX_FILE_BYTES).toBe(Number(key("rag.consumer", "max_file_bytes")));
});

test("ALLOWED_EXTENSIONS mirrors [ingestion].allowed_extensions", () => {
  const server = JSON.parse(key("ingestion", "allowed_extensions")) as string[];
  expect([...ALLOWED_EXTENSIONS].sort()).toEqual([...server].sort());
});
