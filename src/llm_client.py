import os
from dotenv import load_dotenv
from openai import AsyncOpenAI

load_dotenv()

_client = None


def get_openai_client():
    global _client
    if _client is None:  # pragma: no cover
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            raise RuntimeError('OPENAI_API_KEY is not set in the environment.')
        _client = AsyncOpenAI(api_key=api_key)
    return _client  # pragma: no cover
