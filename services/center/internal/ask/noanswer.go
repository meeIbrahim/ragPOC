// Package ask answers questions over the indexed documents: POST /ask streams the sources and
// the answer from the RAG service as server-sent events.
package ask

// NoRelevantAnswer is the RAG service's own reply when retrieval finds nothing. Go sends it
// itself when a selection holds no INDEXED document, so the browser sees the same text either
// way. A copy of NO_RELEVANT_ANSWER in services/rag-service/src/rag/query/service.py;
// TestNoRelevantAnswerMatchesPython fails if they drift.
const NoRelevantAnswer = "No relevant information was found in the indexed documents."
