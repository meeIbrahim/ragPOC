"""LLM over Groq's chat completions (PRD L7: openai/gpt-oss-120b; the key only from GROQ_API_KEY).

Groq's reasoning docs (checked 2026-10-23): gpt-oss takes include_reasoning (bool) and
reasoning_effort (low | medium | high); reasoning_format is not supported for it. Reasoning is
requested off, and only delta.content is forwarded anyway, so a reasoning field in a streamed
chunk can never reach the client (PRD L16).
"""

from __future__ import annotations

import logging
from collections.abc import Iterator

import httpx
from groq import APIError, Groq

from rag.ports import DependencyUnavailable

logger = logging.getLogger(__name__)

_SERVICE = "groq"
# gpt-oss sometimes cites as 【1】 even when the prompt asks for [1]. Mapping single characters
# works when a bracket and its number arrive in different deltas.
_CITATION_BRACKETS = str.maketrans({"【": "[", "】": "]"})


class GroqLLM:
    def __init__(self, client: Groq, model: str, reasoning_effort: str) -> None:
        self._client = client
        self._model = model
        self._reasoning_effort = reasoning_effort

    def stream(self, system: str, user: str) -> Iterator[str]:
        stream = None
        try:
            stream = self._client.chat.completions.create(
                model=self._model,
                messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
                stream=True,
                include_reasoning=False,
                reasoning_effort=self._reasoning_effort,
            )
            for chunk in stream:
                if not chunk.choices:
                    continue
                content = chunk.choices[0].delta.content
                if content:
                    yield content.translate(_CITATION_BRACKETS)
        except (APIError, httpx.HTTPError, OSError) as exc:
            # APIError covers connection, timeout and status errors (429, 401, 5xx).
            logger.warning("Groq call failed: %s", type(exc).__name__)
            raise DependencyUnavailable(_SERVICE, type(exc).__name__) from exc
        finally:
            # Also runs when the caller stops early (client gone), so the HTTP stream is released.
            if stream is not None:
                stream.close()
