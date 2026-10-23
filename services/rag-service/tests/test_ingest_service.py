"""R4 IngestService with port fakes (PRD 5.2; spec E1-E10, A1, D8)."""

from datetime import UTC, datetime

import pytest

from fakes import FakeBlobStore, FakeEmbedder, FakeIndex, FakeParser, FakeRegistry
from rag.adapters.parsers import parser_for
from rag.ingest.errors import PermanentError, TransientError
from rag.ingest.outcome import Failed, Indexed, run_job
from rag.ingest.service import IngestService, IngestSettings
from rag.ports import DependencyUnavailable, IndexedDocument
from rag.transport.status_event import Reason
from rag.transport.watchdog import Deadline
from support import ingest_event

NOW = datetime(2026, 10, 23, 7, 0, tzinfo=UTC)
SETTINGS = IngestSettings(chunk_size=20, chunk_overlap=0, embed_batch_size=2, max_file_bytes=1000)
TEXT = "alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu"  # 4 chunks of <= 20 chars


class Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


class Harness:
    def __init__(self, text=TEXT, indexed=(), settings=SETTINGS, on_embed=None):
        self.event = ingest_event(filename="guide.TXT", size=len(text))
        self.blobs = FakeBlobStore({self.event.bucket_path: text.encode()})
        self.parser = FakeParser(text)
        self.registry = FakeRegistry(indexed)
        self.embedder = FakeEmbedder(on_call=on_embed)
        self.index = FakeIndex()
        self.parsed_for: list[str] = []
        self.service = IngestService(
            registry=self.registry,
            blobs=self.blobs,
            embedder=self.embedder,
            index=self.index,
            settings=settings,
            parser_for=self._parser_for,
            now=lambda: NOW,
        )

    def _parser_for(self, filename):
        self.parsed_for.append(filename)
        parser_for(filename)  # the real dispatch decides support; the fake parser returns the text
        return self.parser

    def run(self, deadline=None):
        return run_job(self.service, self.event, deadline or Deadline.after(60))


def test_happy_path_runs_every_step_in_order_and_registers_the_document():
    h = Harness()
    outcome = h.run()
    assert outcome == Indexed(chunk_count=4)
    assert h.registry.names() == ["exists", "register"]
    assert h.blobs.calls == [("get", (h.event.bucket_path, 1000))]
    assert h.embedder.names() == ["embed_documents", "embed_documents"]  # 4 chunks, batches of 2
    assert h.index.names() == ["delete_by_hash", "upsert"]
    assert h.registry.calls[1][1][0] == IndexedDocument(h.event.hash, "guide.TXT", h.event.bucket_path, 4, NOW)


def test_points_carry_the_hash_filename_index_and_text():
    h = Harness()
    h.run()
    points = h.index.calls[1][1][0]
    assert [p.chunk_index for p in points] == [0, 1, 2, 3]
    assert {p.hash for p in points} == {h.event.hash}
    assert {p.filename for p in points} == {"guide.TXT"}
    assert " ".join(p.text for p in points) == TEXT


def test_an_indexed_hash_is_skipped_without_fetching_E1():
    h = Harness(indexed=[ingest_event().hash])
    assert h.run() == Indexed(chunk_count=None)
    assert h.blobs.calls == [] and h.index.calls == [] and h.embedder.calls == []


def test_an_unsupported_extension_fails_before_the_fetch_E5():
    h = Harness()
    h.event = ingest_event(filename="report.docx")
    assert h.run() == Failed(False, Reason.UNSUPPORTED_TYPE, "unsupported extension '.docx'")
    assert h.blobs.calls == []


def test_an_oversized_event_fails_before_the_fetch_E8():
    h = Harness()
    h.event = ingest_event(filename="guide.txt", size=1001)
    outcome = h.run()
    assert (outcome.retryable, outcome.reason) == (False, Reason.UNSUPPORTED_TYPE)
    assert "too large" in outcome.detail
    assert h.blobs.calls == []


def test_a_lying_size_is_caught_by_the_blob_store_E8():
    h = Harness()
    h.blobs.error = PermanentError(Reason.UNSUPPORTED_TYPE, "file too large (over 1000 bytes)")
    assert h.run() == Failed(False, Reason.UNSUPPORTED_TYPE, "file too large (over 1000 bytes)")
    assert h.index.calls == []


@pytest.mark.parametrize("text", ["", "   \n\t  "])
def test_no_extractable_text_is_empty_text_E6(text):
    h = Harness(text=text)
    outcome = h.run()
    assert (outcome.retryable, outcome.reason) == (False, Reason.EMPTY_TEXT)
    assert h.embedder.calls == [] and h.index.calls == []


def test_a_corrupt_file_is_permanent_E7():
    h = Harness()
    h.parser.error = PermanentError(Reason.CORRUPT_FILE, "unreadable PDF (PdfReadError)")
    assert h.run() == Failed(False, Reason.CORRUPT_FILE, "unreadable PDF (PdfReadError)")


def test_a_missing_object_is_retryable_internal_A1():
    h = Harness()
    h.blobs.error = TransientError(Reason.INTERNAL, "object missing")
    assert h.run() == Failed(True, Reason.INTERNAL, "object missing")


@pytest.mark.parametrize("failing", ["embedder", "index"])
def test_dependency_outages_are_retryable_E9(failing):
    h = Harness()
    getattr(h, failing).error = DependencyUnavailable(failing, "ConnectError")
    outcome = h.run()
    assert (outcome.retryable, outcome.reason) == (True, Reason.DEPENDENCY_UNAVAILABLE)
    assert h.registry.names() == ["exists"]


def test_the_deadline_is_checked_between_embedding_batches_E10():
    clock = Clock()
    h = Harness(on_embed=lambda: setattr(clock, "t", clock.t + 70))  # one batch outlasts the 60s deadline
    outcome = h.run(Deadline.after(60, clock))
    assert (outcome.retryable, outcome.reason) == (True, Reason.JOB_TIMEOUT)
    # 4 chunks in batches of 2: the check before the 2nd batch stops the job.
    assert h.embedder.names() == ["embed_documents"]
    assert h.index.calls == [] and h.registry.names() == ["exists"]


def test_an_already_expired_deadline_stops_before_the_fetch():
    clock = Clock()
    deadline = Deadline.after(0, clock)
    h = Harness()
    outcome = h.run(deadline)
    assert outcome.reason is Reason.JOB_TIMEOUT
    assert h.blobs.calls == []


def test_a_crash_after_upsert_then_a_rerun_leaves_no_duplicates_E2():
    h = Harness()
    register = h.registry.register

    def crash(document):
        raise OSError("disk full")

    h.registry.register = crash
    first = h.run()
    assert (first.retryable, first.reason) == (True, Reason.INTERNAL)
    assert h.index.count_for(h.event.hash) == 4
    h.registry.register = register
    assert h.run() == Indexed(chunk_count=4)
    assert h.index.count_for(h.event.hash) == 4


def test_fewer_chunks_on_reindex_leave_no_stale_points_E3():
    h = Harness()
    h.run()
    h.registry.clear()
    h.parser.text = "short text"
    h.run()
    assert h.index.count_for(h.event.hash) == 1


def test_delete_happens_before_upsert_and_register_after():
    h = Harness()
    order = []
    h.index.delete_by_hash = lambda hash: order.append("delete")
    h.index.upsert = lambda points: order.append("upsert")
    original_register = h.registry.register
    h.registry.register = lambda doc: (order.append("register"), original_register(doc))
    h.run()
    assert order == ["delete", "upsert", "register"]
