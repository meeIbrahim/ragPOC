# ragPOC (Pucho)

- `GROQ_API_KEY="$GROQ" make stack`: the whole system in containers; the UI is at http://localhost:8080 (needs Ollama running on the host).
- `make demo`: the scripted demo against the running stack (pre-flight, upload, merge, asks); see `docs/demo.md`.
- `make demo-reset`: deletes this checkout's stack data after you type `reset`; then `make stack` starts clean.
- `cd services/web && npm run dev`: the UI on http://localhost:5173 against the stack's API on :8080.
