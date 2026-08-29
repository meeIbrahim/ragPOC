import ollama
from core import config_reader


def embed(text: str) -> list[float]:
    model = config_reader.get_config().rag.model
    response = ollama.embed(model=model, input=text)
    return response["embeddings"][0]
