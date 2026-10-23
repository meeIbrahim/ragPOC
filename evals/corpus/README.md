# Retrieval eval corpus

Public, permissively licensed documents only. Indexed text is sent to Groq when queried (PRD 5.4),
so never add client or production documents here. Files are used unmodified; their SHA-256 is
the expected hash in `../questions.jsonl`.

| File | Source | Licence |
|---|---|---|
| `nist-csf-2.0.pdf` | NIST CSWP 29, *The NIST Cybersecurity Framework (CSF) 2.0*, 2024-02-26, https://nvlpubs.nist.gov/nistpubs/CSWP/NIST.CSWP.29.pdf | US Government work, public domain in the US |
| `jekyll-and-hyde.txt` | Robert Louis Stevenson, *The Strange Case of Dr Jekyll and Mr Hyde*, Project Gutenberg eBook #43, https://www.gutenberg.org/ebooks/43 | Public domain in the US; Project Gutenberg licence text kept in the file |
| `rust-ownership.md` | *The Rust Programming Language*, chapter 4.1 "What Is Ownership?", https://github.com/rust-lang/book (`src/ch04-01-what-is-ownership.md`) | MIT or Apache-2.0 |
| `kubernetes-pods.md` | Kubernetes documentation, "Pods", https://github.com/kubernetes/website (`content/en/docs/concepts/workloads/pods/_index.md`) | CC BY 4.0, © The Kubernetes Authors |

Retrieved 2026-10-23.

Notes:
- The PDF's extracted text has some flaws: a few split words ("Cur rent"), page headers on
  every page, and table cells interleaved by column (the Tiers table). These are kept on
  purpose, as real-world input.
- The PDF is 1.5 MB, over the spec's 1 MB guideline; its extracted text is about 70 KB.
