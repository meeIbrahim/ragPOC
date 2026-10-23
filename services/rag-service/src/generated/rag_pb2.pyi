from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class AskRequest(_message.Message):
    __slots__ = ("query", "user_id", "chat_history", "document_ids")
    QUERY_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    CHAT_HISTORY_FIELD_NUMBER: _ClassVar[int]
    DOCUMENT_IDS_FIELD_NUMBER: _ClassVar[int]
    query: str
    user_id: str
    chat_history: _containers.RepeatedScalarFieldContainer[str]
    document_ids: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, query: _Optional[str] = ..., user_id: _Optional[str] = ..., chat_history: _Optional[_Iterable[str]] = ..., document_ids: _Optional[_Iterable[str]] = ...) -> None: ...

class AskResponse(_message.Message):
    __slots__ = ("answer", "context_used")
    ANSWER_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_USED_FIELD_NUMBER: _ClassVar[int]
    answer: str
    context_used: _containers.RepeatedCompositeFieldContainer[Source]
    def __init__(self, answer: _Optional[str] = ..., context_used: _Optional[_Iterable[_Union[Source, _Mapping]]] = ...) -> None: ...

class AskStreamRequest(_message.Message):
    __slots__ = ("query", "user_id", "chat_history", "document_ids")
    QUERY_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    CHAT_HISTORY_FIELD_NUMBER: _ClassVar[int]
    DOCUMENT_IDS_FIELD_NUMBER: _ClassVar[int]
    query: str
    user_id: str
    chat_history: _containers.RepeatedScalarFieldContainer[str]
    document_ids: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, query: _Optional[str] = ..., user_id: _Optional[str] = ..., chat_history: _Optional[_Iterable[str]] = ..., document_ids: _Optional[_Iterable[str]] = ...) -> None: ...

class AskStreamResponse(_message.Message):
    __slots__ = ("answer", "context_used")
    ANSWER_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_USED_FIELD_NUMBER: _ClassVar[int]
    answer: str
    context_used: _containers.RepeatedCompositeFieldContainer[Source]
    def __init__(self, answer: _Optional[str] = ..., context_used: _Optional[_Iterable[_Union[Source, _Mapping]]] = ...) -> None: ...

class Source(_message.Message):
    __slots__ = ("document_id", "snippet", "similarity_score", "filename")
    DOCUMENT_ID_FIELD_NUMBER: _ClassVar[int]
    SNIPPET_FIELD_NUMBER: _ClassVar[int]
    SIMILARITY_SCORE_FIELD_NUMBER: _ClassVar[int]
    FILENAME_FIELD_NUMBER: _ClassVar[int]
    document_id: str
    snippet: str
    similarity_score: float
    filename: str
    def __init__(self, document_id: _Optional[str] = ..., snippet: _Optional[str] = ..., similarity_score: _Optional[float] = ..., filename: _Optional[str] = ...) -> None: ...
