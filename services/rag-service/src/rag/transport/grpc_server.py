"""The gRPC transport for QueryService (PRD 4.4): a thin mapping, no retrieval logic here.

Status codes: InvalidQuery -> INVALID_ARGUMENT, QueryUnavailable -> UNAVAILABLE, anything else
-> INTERNAL with a generic message (the details are logged, never sent). user_id and
chat_history are ignored in v1 (PRD L8, L9).
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from concurrent import futures

import grpc
from grpc_reflection.v1alpha import reflection

from generated import rag_pb2, rag_pb2_grpc
from rag.ports import Hit
from rag.query.service import InvalidQuery, QueryService, QueryUnavailable

logger = logging.getLogger(__name__)

SERVICE_NAME = rag_pb2.DESCRIPTOR.services_by_name["RagEngineService"].full_name


def _source(hit: Hit) -> rag_pb2.Source:
    return rag_pb2.Source(
        document_id=hit.hash, snippet=hit.text, similarity_score=hit.score, filename=hit.filename
    )


def _abort(context: grpc.ServicerContext, exc: Exception) -> None:
    """Ends the RPC with the status for exc. context.abort raises, so this never returns."""
    if isinstance(exc, InvalidQuery):
        context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
    if isinstance(exc, QueryUnavailable):
        logger.warning("query failed: %s", exc)
        context.abort(grpc.StatusCode.UNAVAILABLE, str(exc))
    logger.exception("query failed unexpectedly", exc_info=exc)
    context.abort(grpc.StatusCode.INTERNAL, "internal error")


class RagEngineServicer(rag_pb2_grpc.RagEngineServiceServicer):
    def __init__(self, service: QueryService) -> None:
        self._service = service

    def Ask(self, request: rag_pb2.AskRequest, context: grpc.ServicerContext) -> rag_pb2.AskResponse:
        try:
            answer, hits = self._service.ask(request.query, list(request.document_ids))
        except Exception as exc:
            _abort(context, exc)
        return rag_pb2.AskResponse(answer=answer, context_used=[_source(h) for h in hits])

    def AskStream(
        self, request: rag_pb2.AskStreamRequest, context: grpc.ServicerContext
    ) -> Iterator[rag_pb2.AskStreamResponse]:
        messages = None
        try:
            messages = self._service.ask_stream(request.query, list(request.document_ids))
            for message in messages:
                if not context.is_active():
                    logger.info("client went away; stopping the answer stream")
                    return
                yield rag_pb2.AskStreamResponse(
                    answer=message.answer, context_used=[_source(h) for h in message.sources]
                )
        except Exception as exc:
            _abort(context, exc)
        finally:
            # Closing the answer generator closes the LLM stream behind it (no more Groq reads).
            if messages is not None:
                messages.close()


def serve(service: QueryService, port: int, max_workers: int, host: str = "[::]") -> tuple[grpc.Server, int]:
    """Start the server; returns it and the bound port (port 0 picks a free one)."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    rag_pb2_grpc.add_RagEngineServiceServicer_to_server(RagEngineServicer(service), server)
    # Reflection lets grpcurl call the service without the .proto file.
    reflection.enable_server_reflection((SERVICE_NAME, reflection.SERVICE_NAME), server)
    bound = server.add_insecure_port(f"{host}:{port}")
    server.start()
    return server, bound
