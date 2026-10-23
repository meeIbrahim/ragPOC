"""Retrieval eval (PRD 7 Phase 1; spec R10, D7, D11). Retrieval only: Groq is never called.

    make eval                      # from the repo root; needs `make run_worker` running
    cd services/rag-service && uv run python ../../evals/run_eval.py [--config PATH]

1. Checks questions.jsonl: each expected_hash is its file's SHA-256, and no question shares a
   5-word sequence with its document (the leakage gate). Any problem stops the run (exit 2).
2. Ingests every corpus file through rag.tools.ingest_file, skipping ones already indexed.
3. Retrieves top_k for every question with min_score 0, so every score is visible.
4. Writes evals/results/<UTC date>-<chunk_size>-<overlap>-k<top_k>.md and prints a summary.

Already indexed files are skipped, so after changing chunk_size or chunk_overlap run
`make reset_index ARGS=--yes` first: otherwise the report carries the new values but measures the
old chunks.

Exit codes: 0 every document at or above the bar, 1 a document below it, 2 setup problem.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from leakage import DEFAULT_N, shared_ngrams
from rag.adapters.parsers import parser_for
from rag.core import config_reader, wiring
from rag.core.config_reader import Config
from rag.ingest.errors import IngestError
from rag.tools import ingest_file

EVALS = Path(__file__).resolve().parent
# The PRD bar: the expected hash in the top-k for at least 8 of 10 questions, per document (D7).
PASS_RATIO = 0.8
EXIT_PASS, EXIT_BELOW_BAR, EXIT_SETUP = 0, 1, 2


@dataclass(frozen=True)
class Question:
    id: str
    question: str
    file: str
    expected_hash: str | None  # None for negatives


@dataclass(frozen=True)
class Result:
    question: Question
    rank: int | None  # 1-based rank of the expected hash in the top-k, None if missing
    hit_score: float | None
    top_score: float | None


class _NoLLM:
    """Retrieval only: the eval must never send text to Groq."""

    def stream(self, system, user):
        raise AssertionError("the retrieval eval must not call the LLM")


def _load(path: Path, negatives: bool) -> list[Question]:
    questions = []
    for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        row = json.loads(line)
        questions.append(Question(
            id=row["id"],
            question=row["question"],
            file="" if negatives else row["file"],
            expected_hash=None if negatives else row["expected_hash"],
        ))
    return questions


def corpus_files(corpus: Path) -> list[Path]:
    return sorted(p for p in corpus.iterdir() if p.is_file() and p.name != "README.md" and not p.name.startswith("."))


def check_questions(questions: list[Question], corpus: Path) -> list[str]:
    """Every problem with the question set, including leaked phrases (empty list = clean)."""
    problems: list[str] = []
    texts: dict[str, str] = {}
    for q in questions:
        path = corpus / q.file
        if not path.is_file():
            problems.append(f"{q.id}: file {q.file!r} is not in {corpus}")
            continue
        data = path.read_bytes()
        if hashlib.sha256(data).hexdigest() != q.expected_hash:
            problems.append(f"{q.id}: expected_hash is not the SHA-256 of {q.file}")
        if q.file not in texts:
            try:
                texts[q.file] = parser_for(q.file).parse(data)
            except IngestError as exc:
                problems.append(f"{q.id}: cannot parse {q.file}: {exc}")
                continue
        for phrase in sorted(shared_ngrams(q.question, texts[q.file], DEFAULT_N)):
            problems.append(f"{q.id}: copies {DEFAULT_N}+ words from {q.file}: {phrase!r}")
    return problems


def ingest_corpus(config: Config, files: list[Path]) -> list[str]:
    registry = wiring.registry(config)
    minio, redis_client = ingest_file.connect(config)
    wait = (config.rag.consumer.job_timeout + config.rag.consumer.watchdog_grace).total_seconds()
    failures = []
    for path in files:
        if registry.exists(hashlib.sha256(path.read_bytes()).hexdigest()):
            print(f"already indexed: {path.name}")
            continue
        print(f"ingesting {path.name} (waits up to {wait:.0f}s)")
        if ingest_file.run(path, config, minio, redis_client, wait=wait) != ingest_file.EXIT_OK:
            failures.append(path.name)
    return failures


def evaluate(config: Config, questions: list[Question], negatives: list[Question]) -> tuple[list[Result], list[Result]]:
    service = wiring.query_service(config, llm_adapter=_NoLLM())

    def run(q: Question) -> Result:
        hits = service.retrieve(q.question, [], min_score=0.0)
        hashes = [h.hash for h in hits]
        rank = hashes.index(q.expected_hash) + 1 if q.expected_hash in hashes else None
        return Result(q, rank, hits[rank - 1].score if rank else None, hits[0].score if hits else None)

    return [run(q) for q in questions], [run(q) for q in negatives]


def report(config: Config, results: list[Result], negatives: list[Result]) -> tuple[str, bool]:
    by_file: dict[str, list[Result]] = defaultdict(list)
    for r in results:
        by_file[r.question.file].append(r)
    passed = True
    lines = [
        f"# Retrieval eval {datetime.now(UTC):%Y-%m-%d %H:%M} UTC",
        "",
        f"- Model `{config.rag.embedding_model}` ({config.rag.embedding_dim} dims), "
        f"chunk_size {config.rag.chunk_size}, chunk_overlap {config.rag.chunk_overlap}, top_k {config.rag.top_k}",
        f"- Query instruction: `{config.rag.query_instruction}`",
        f"- Bar: expected hash in the top {config.rag.top_k} for at least {PASS_RATIO:.0%} of each document's questions",
        "",
        "| Document | Hits | Questions | Result |",
        "|---|---|---|---|",
    ]
    for file, rows in sorted(by_file.items()):
        hits = sum(1 for r in rows if r.rank)
        ok = hits >= PASS_RATIO * len(rows)
        passed &= ok
        lines.append(f"| {file} | {hits} | {len(rows)} | {'pass' if ok else 'BELOW BAR'} |")
    total = sum(1 for r in results if r.rank)
    lines += ["", f"Overall: {total}/{len(results)}.", "", "## Questions", "",
              "| Id | File | Rank | Correct-hit score | Top score |", "|---|---|---|---|---|"]
    for r in results:
        lines.append(f"| {r.question.id} | {r.question.file} | {r.rank or 'miss'} | "
                     f"{_score(r.hit_score)} | {_score(r.top_score)} |")
    lines += ["", "## Negatives (off-topic)", "", "| Id | Top score |", "|---|---|"]
    lines += [f"| {r.question.id} | {_score(r.top_score)} |" for r in negatives]

    correct = [r.hit_score for r in results if r.hit_score is not None]
    negative_tops = [r.top_score for r in negatives if r.top_score is not None]
    lines += ["", "## min_score calibration", ""]
    if correct:
        lines.append(f"- Lowest correct-hit score: {min(correct):.3f}")
    if negative_tops:
        lines.append(f"- Highest negative top score: {max(negative_tops):.3f}")
    if correct and negative_tops:
        if max(negative_tops) < min(correct):
            lines.append("- The ranges do not overlap: any min_score between them separates them.")
        else:
            lines.append("- The ranges OVERLAP: no min_score rejects every negative without dropping a correct hit.")
    return "\n".join(lines) + "\n", passed


def _score(value: float | None) -> str:
    return "-" if value is None else f"{value:.3f}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", help="path to config.toml (default: $RAG_CONFIG, else the repo's config.toml)")
    parser.add_argument("--corpus", type=Path, default=EVALS / "corpus")
    parser.add_argument("--questions", type=Path, default=EVALS / "questions.jsonl")
    parser.add_argument("--negatives", type=Path, default=EVALS / "negatives.jsonl")
    parser.add_argument("--results", type=Path, default=EVALS / "results")
    parser.add_argument("--check-only", action="store_true", help="only validate the questions (leakage gate)")
    args = parser.parse_args(argv)

    questions = _load(args.questions, negatives=False)
    negatives = _load(args.negatives, negatives=True) if args.negatives.exists() else []
    problems = check_questions(questions, args.corpus)
    if problems:
        print("question set rejected:", *problems, sep="\n  ", file=sys.stderr)
        return EXIT_SETUP
    print(f"{len(questions)} questions and {len(negatives)} negatives pass the checks")
    if args.check_only:
        return EXIT_PASS

    try:
        config = config_reader.load(args.config)
    except config_reader.ConfigError as exc:
        print(exc, file=sys.stderr)
        return EXIT_SETUP
    failures = ingest_corpus(config, corpus_files(args.corpus))
    if failures:
        print("ingest failed for:", ", ".join(failures), file=sys.stderr)
        return EXIT_SETUP

    results, negative_results = evaluate(config, questions, negatives)
    text, passed = report(config, results, negative_results)
    args.results.mkdir(parents=True, exist_ok=True)
    out = args.results / (f"{datetime.now(UTC):%Y%m%dT%H%M}-{config.rag.chunk_size}-"
                          f"{config.rag.chunk_overlap}-k{config.rag.top_k}.md")
    out.write_text(text, encoding="utf-8")
    print(text)
    print(f"written to {out}")
    return EXIT_PASS if passed else EXIT_BELOW_BAR


if __name__ == "__main__":
    sys.exit(main())
