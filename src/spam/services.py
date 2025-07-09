import hashlib
import logging
from foxglove import glove
from openai import AsyncOpenAI
from pydantic import BaseModel
from typing import Optional

from src.llm_client import get_openai_client
from src.schemas.messages import EmailSendModel
from src.spam.llm_prompt import LLMPromptTemplate

logger = logging.getLogger('spam_check')


class SpamCheckResult(BaseModel):
    spam: bool
    reason: str


class OpenAISpamEmailService:
    text_format: type[BaseModel] = SpamCheckResult
    model: str

    def __init__(self, client: AsyncOpenAI = None):
        if client is None:
            client = get_openai_client()  # pragma: no cover
        self.client: AsyncOpenAI = client
        self.model = glove.settings.llm_model_name

    def _prepare_prompt(self, prompt_template: LLMPromptTemplate) -> tuple[str, str]:
        instruction = prompt_template.render_sys_prompt()
        prompt = prompt_template.render_prompt()
        return prompt, instruction

    async def is_spam_email(self, prompt_template: LLMPromptTemplate) -> SpamCheckResult:
        prompt, instruction = self._prepare_prompt(prompt_template)
        response = await self.client.responses.parse(
            model=self.model,
            input=prompt,
            instructions=instruction,
            text_format=self.text_format,
        )
        result = response.output_parsed
        return result


class SpamCacheService:
    def __init__(self, redis_client):
        self.redis = redis_client
        self.cache_ttl = 365 * 24 * 60 * 60

    def get_cache_key(self, m: EmailSendModel) -> str:
        main_message = m.context.get('main_message__render', '')
        main_message_hash = hashlib.sha256(main_message.encode('utf-8')).hexdigest()
        return f'spam_content:{main_message_hash}:{m.company_code}'

    async def get(self, m: EmailSendModel) -> Optional[SpamCheckResult]:
        key = self.get_cache_key(m)
        spam_reason = await self.redis.get(key)
        if spam_reason:
            return SpamCheckResult(spam=True, reason=spam_reason)
        return None

    async def set(self, m: EmailSendModel, reason: str):
        key = self.get_cache_key(m)
        await self.redis.set(key, reason, expire=self.cache_ttl)
