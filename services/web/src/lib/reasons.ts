// failure_reason code to row text (design 3.4). The raw code goes in the row's title only.
const texts: Readonly<Record<string, string>> = {
  UNSUPPORTED_TYPE: "This file type or size isn't supported. Use PDF, TXT or MD up to 10 MB.",
  EMPTY_TEXT: "No text found in this file. Scanned PDFs need OCR, which isn't supported.",
  CORRUPT_FILE: "The file couldn't be read. It may be damaged.",
  DEPENDENCY_UNAVAILABLE: "Indexing services were unavailable on every attempt.",
  JOB_TIMEOUT: "Indexing took longer than 15 minutes. Try a smaller file.",
  POISON_MESSAGE: "Indexing kept crashing on this file.",
  INTERNAL: "Indexing failed with an internal error.",
  UPLOAD_FAILED: "The upload couldn't be processed. Try uploading again.",
};

export const FALLBACK_REASON = "Indexing failed.";

export function reasonText(code: string | undefined): string {
  return code && Object.hasOwn(texts, code) ? (texts[code] as string) : FALLBACK_REASON;
}
