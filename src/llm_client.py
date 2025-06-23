from foxglove import glove
from openai import AsyncOpenAI

_client = None


def get_openai_client():
    global _client
    if _client is None:  # pragma: no cover
        api_key = glove.settings.openai_api_key
        if not api_key:
            raise RuntimeError('OPENAI_API_KEY is not set in the environment.')
        _client = AsyncOpenAI(api_key=api_key)
    return _client  # pragma: no cover
