import hashlib
import logging
from foxglove import glove
from openai import AsyncOpenAI
from pydantic import BaseModel

from src.llm_client import get_openai_client
from src.schemas.messages import EmailSendModel
from src.spam.llm_prompt import LLMPromptTemplate

logger = logging.getLogger('spam_check')


class SpamCheckResult(BaseModel):
    spam: bool
    reason: str


class BaseSpamEmailService:
    async def is_spam_email(self, m: EmailSendModel) -> SpamCheckResult:
        logger.info(f"Starting spam check for content hash of group: {m.uid}")

        main_message = m.context.get('main_message__render', '')
        main_message_hash = hashlib.sha256(main_message.encode('utf-8')).hexdigest()

        spam_redis_key = f'spam_content:{main_message_hash}:{m.company_code}'
        spam_reason = await glove.redis.get(spam_redis_key)
        if spam_reason:
            logger.info("Spam key found in Redis for content hash, skipping LLM.")
            return SpamCheckResult(spam=True, reason=spam_reason)
        result = await self._detect_spam(m)
        if result.spam:
            logger.info("Email content flagged as spam, caching reason.")
            one_year_seconds = 365 * 24 * 60 * 60
            await glove.redis.set(spam_redis_key, result.reason, expire=one_year_seconds)
        else:
            logger.info("Email content passed spam check.")  # pragma: no cover
        return result

    async def _detect_spam(self, m: EmailSendModel) -> SpamCheckResult:
        raise NotImplementedError


class OpenAISpamEmailService(BaseSpamEmailService):
    LLMPrompt: type[LLMPromptTemplate] = LLMPromptTemplate
    model: str = "gpt-4o"
    text_format: type[BaseModel] = SpamCheckResult

    def __init__(self, client: AsyncOpenAI = None):
        if client is None:
            client = get_openai_client()  # pragma: no cover
        self.client: AsyncOpenAI = client

    def _prepare_prompt(self, m: EmailSendModel) -> tuple[str, str]:
        prompt_template = self.LLMPrompt(m)
        instruction = prompt_template.render_sys_prompt()
        prompt = prompt_template.render_prompt()
        return prompt, instruction

    async def _detect_spam(self, m: EmailSendModel) -> SpamCheckResult:
        prompt, instruction = self._prepare_prompt(m)
        response = await self.client.responses.parse(
            model=self.model,
            input=prompt,
            instructions=instruction,
            text_format=self.text_format,
        )
        result = response.output_parsed
        return result
