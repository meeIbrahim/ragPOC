// The browser can't read the server config, so these mirror it. limits.test.ts fails when they
// drift from deploy/config.compose.toml; the server's 400s stay the authority.

/** [rag].max_document_ids */
export const MAX_SELECTED = 100;

/** [rag].max_query_chars, counted in code points like the server's utf8.RuneCountInString. */
export const MAX_QUERY_CHARS = 2000;

/** The question counter shows past this many code points (design 5.1). */
export const QUERY_COUNTER_FROM = 1800;

/** [rag.consumer].max_file_bytes */
export const MAX_FILE_BYTES = 10_485_760;

/** [ingestion].allowed_extensions, compared case-insensitively. */
export const ALLOWED_EXTENSIONS: readonly string[] = [".pdf", ".txt", ".md"];

/** GET /documents poll period while anything is uploading or processing (design 3.5). */
export const POLL_INTERVAL_MS = 2000;

/** Failed polls in a row before "Can't reach the server" (design 3.5). */
export const POLL_STALE_AFTER = 3;

/** Polls after confirm with no entry listing the upload before "Taking longer than expected" (design 3.3 4a). */
export const NOT_LISTED_AFTER_POLLS = 15;

/** A processing row reads "Taking longer than usual" after this long (design 3.2). */
export const SLOW_PROCESSING_MS = 5 * 60 * 1000;

/** Relative times re-render on this tick (spec A9). */
export const NOW_TICK_MS = 15_000;

/** Must equal --toast-timeout in tokens.css (styles.test.ts). */
export const TOAST_TIMEOUT_MS = 5000;

/** Sources shown at most (design 5.2). */
export const MAX_SOURCES_SHOWN = 5;
