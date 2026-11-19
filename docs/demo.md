# Pucho demo runbook

For whoever presents Pucho to the team or a client. The UI runs from the compose stack on one
laptop. `make demo` checks the same flow with curl before the call; this page covers what curl
can't see: rendering, live status, streaming, Stop and the error cards.

## Data rule

Questions and the retrieved chunks go to Groq. Use only public or synthetic documents: the files
in `evals/corpus/` and `demo/pucho-demo.pdf`. Never upload client or personal files. The header
note "Demo build · use public documents only" stays visible for this reason.

## Setup

1. Ollama runs on the host with the embedding model: `ollama list` shows `qwen3-embedding:4b`.
2. Start the stack with the Groq key from your shell (never write it to a file):
   `GROQ_API_KEY="$GROQ" make stack`
3. Open http://localhost:8080.

The Makefile prints the compose project (the checkout folder name). Only one stack can run at a
time (ports 8080, 9000, 50051): run `make down` in the other checkout first.

## Pre-flight (about 10 minutes before the call)

1. If the list holds old FAILED rows you don't want on screen, reset first. Failed documents
   can't be deleted any other way, and re-uploading the same content never retries it:
   `make demo-reset` (type `reset` when asked), then `GROQ_API_KEY="$GROQ" make stack`.
   On Linux the data folders may be root-owned; then the reset needs `sudo`.
2. `make demo`. Every line must say PASS. What a FAIL means:
   - pre-flight "center doesn't answer": the stack isn't up.
   - pre-flight "Ollama": start Ollama or pull the model.
   - pre-flight "rag-server is not healthy": the stack was started without `GROQ_API_KEY`.
   - step 4 "the answer failed … Check GROQ_API_KEY": the key is wrong or Groq is down.
3. `make demo` leaves `demo-…` rows in the list, and later uploads of the same files merge into
   them. For a clean screen, run `make demo` first, then `make demo-reset` and `make stack`.

## Questions to type

The same questions `make demo` asks, so they are known to work:

| Purpose | Question |
|---|---|
| Ask over everything | What is the tiniest unit of compute that the cluster lets you create and manage? |
| Ask about the PDF | Which chapter of the novel does the Pucho demo sample reproduce? |
| Nothing relevant | How long should sourdough dough proof before baking? |

## Browser walk-through

Start from a clean list: `make demo` indexes the same files, so after it every upload in step 1
just merges into an existing document. Run `make demo-reset`, then `GROQ_API_KEY="$GROQ" make stack`.

| # | Do | Expect |
|---|---|---|
| 1 | Upload `evals/corpus/kubernetes-pods.md`, `evals/corpus/rust-ownership.md` and `demo/pucho-demo.pdf` | Each row: Uploading with a percentage, then Processing with a timer, then Indexed. The status region announces "… is indexed." |
| 2 | Upload a copy of `rust-ownership.md` under another name | After a few seconds the row folds into the original, which flashes, and a toast says it is already uploaded under the original name. |
| 3 | Drag any `.png` onto the drop zone (the file picker hides other types) | No row; the error under the upload button says the type isn't supported. |
| 4 | With nothing selected, ask the "everything" question | "Searching documents…", then Sources (rank 1 outlined), then the answer streams with amber [n] pills; Done shows the source count and time. Hover a pill: its source lights up. |
| 5 | Select only the PDF, ask the PDF question | The scope line says "Searching 1 document"; every source is the PDF. |
| 6 | Ask the "nothing relevant" question | A calm grey card, no sources, no red. |
| 7 | Ask again and press Stop (or Escape) while it streams | The partial answer stays with "Stopped."; the question is back in the box. |
| 8 | Groq unreachable: `make down`, then `GROQ_API_KEY=bad-key make stack` (rag-server starts with any key; Groq rejects it), then ask anything | Sources show, then a red error card with "Try again", which has focus. Afterwards: `make down`, then `GROQ_API_KEY="$GROQ" make stack`. |

## Dev server check

`cd services/web && npm run dev` serves the UI on http://localhost:5173 against the stack's API.
MinIO's CORS allows both `:8080` and `:5173`, so uploads work from either.

## Teardown

`make down` stops the stack and keeps the data. `make demo-reset` deletes it.

## Results

| Date | Check | Result |
|---|---|---|
| 2026-11-19 | `make demo-reset` answered `no`, empty input and `RESET` | Aborted; nothing deleted (275 files, 2 volumes kept) |
| 2026-11-19 | `make demo-reset` answered `reset` | Volumes and data folders emptied (0 files, 0 volumes) |
| 2026-11-19 | `make demo` on the fresh stack, then again on the same stack | All PASS both times; the second run merged into the first run's entries |
| 2026-11-19 | `make demo` with Ollama stopped | FAIL at the Ollama pre-flight, before any upload |
| 2026-11-19 | CORS preflight from `:5173` and `:8080` to MinIO | Allowed, each origin echoed |
| 2026-11-19 | PDF question over all documents with `jekyll-and-hyde.txt` indexed | PDF ranked first (0.624; the book's best chunk 0.444) |
