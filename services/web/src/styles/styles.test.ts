import { readFileSync, readdirSync } from "node:fs";
import { join, relative } from "node:path";
import { describe, expect, test } from "vitest";
import { TOAST_TIMEOUT_MS } from "../limits";

const srcDir = join(__dirname, "..");

function files(ext: RegExp): string[] {
  return readdirSync(srcDir, { recursive: true, encoding: "utf8" })
    .filter((f) => ext.test(f))
    .map((f) => join(srcDir, f));
}

const tokensCss = readFileSync(join(__dirname, "tokens.css"), "utf8");

describe("design tokens are the only source of style values (spec D2)", () => {
  const sheets = files(/\.css$/).filter((f) => !f.endsWith("tokens.css"));

  test("there are style sheets to check", () => {
    expect(sheets.length).toBeGreaterThan(0);
  });

  test.each(sheets.map((f) => [relative(srcDir, f), f]))("%s has no hex color literal", (_name, file) => {
    const hex = readFileSync(file, "utf8").match(/#[0-9a-fA-F]{3,8}\b/g) ?? [];
    expect(hex, "colors must come from tokens.css").toEqual([]);
  });

  test.each(sheets.map((f) => [relative(srcDir, f), f]))("%s has no px value other than ±1px", (_name, file) => {
    const px = (readFileSync(file, "utf8").match(/-?\d*\.?\d+px\b/g) ?? []).filter((v) => v !== "1px" && v !== "-1px");
    expect(px, "sizes must come from tokens.css (±1px hairlines excepted)").toEqual([]);
  });
});

test("tokens.css declares every layout width the design names (review N2)", () => {
  for (const name of ["--size-docs-panel: 360px", "--size-answer-max: 760px", "--size-app-min: 1280px"]) {
    expect(tokensCss).toContain(name);
  }
});

test("reduced motion sets every duration token to 0ms (design 6)", () => {
  const reduced = tokensCss.slice(tokensCss.indexOf("prefers-reduced-motion"));
  for (const name of ["--duration-fast", "--duration-base", "--duration-stagger"]) {
    expect(reduced).toMatch(new RegExp(`${name}: 0ms`));
  }
});

test("the JS toast timeout matches --toast-timeout", () => {
  const m = tokensCss.match(/--toast-timeout:\s*(\d+(?:\.\d+)?)(ms|s);/);
  expect(m, "--toast-timeout must be declared in tokens.css").not.toBeNull();
  const [, value, unit] = m as RegExpMatchArray;
  expect(TOAST_TIMEOUT_MS).toBe(Number(value) * (unit === "s" ? 1000 : 1));
});

test("no dangerouslySetInnerHTML anywhere in src (model output and filenames stay text)", () => {
  const offenders = files(/\.(ts|tsx)$/)
    .filter((f) => !f.endsWith("styles.test.ts"))
    .filter((f) => readFileSync(f, "utf8").includes("dangerouslySetInnerHTML"));
  expect(offenders.map((f) => relative(srcDir, f))).toEqual([]);
});
