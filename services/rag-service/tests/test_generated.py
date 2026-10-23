"""The committed gRPC stubs import cleanly and carry the Phase 1 fields (PRD 4.4)."""

from generated import rag_pb2, rag_pb2_grpc


def test_the_grpc_module_imports_the_messages_from_the_generated_package():
    # protoc's default `from rag.v1 import rag_pb2` would resolve to this service's `rag` package.
    assert rag_pb2_grpc.rag__pb2 is rag_pb2


def test_requests_carry_document_ids():
    for message in (rag_pb2.AskRequest, rag_pb2.AskStreamRequest):
        request = message(query="q", document_ids=["a" * 64])
        assert list(request.document_ids) == ["a" * 64]


def test_source_carries_filename():
    source = rag_pb2.Source(document_id="h", snippet="s", similarity_score=0.5, filename="guide.md")
    assert source.filename == "guide.md"


def test_the_service_keeps_its_full_name():
    assert rag_pb2.DESCRIPTOR.services_by_name["RagEngineService"].full_name == "rag.v1.RagEngineService"
