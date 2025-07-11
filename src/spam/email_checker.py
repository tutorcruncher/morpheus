import logging
from html import escape

from src.render.main import MessageDef, render_email
from src.schemas.messages import EmailSendModel
from src.spam.llm_prompt import LLMPromptTemplate
from src.spam.services import OpenAISpamEmailService, SpamCacheService

logger = logging.getLogger('spam.email_checker')


class EmailSpamChecker:
    def __init__(self, spam_service: OpenAISpamEmailService, cache_service: SpamCacheService):
        self.spam_service = spam_service
        self.cache_service = cache_service

    async def check_spam(self, m: EmailSendModel):
        spam_result = await self.cache_service.get(m)
        if spam_result:
            return spam_result

        # prepare email info for spam check
        recipient = m.recipients[0] if m.recipients else None
        context = dict(m.context, **(recipient.context if recipient and hasattr(recipient, "context") else {}))
        headers = dict(m.headers, **(recipient.headers if recipient and hasattr(recipient, "headers") else {}))
        message_def = MessageDef(
            first_name=recipient.first_name if recipient else "",
            last_name=recipient.last_name if recipient else "",
            main_template=m.main_template,
            mustache_partials=m.mustache_partials or {},
            macros=m.macros or {},
            subject_template=m.subject_template,
            context=context,
            headers=headers,
        )
        email_info = render_email(message_def)
        company_name = m.context.get("company_name", "")
        prompt_template = LLMPromptTemplate(email_info, company_name)
        escaped_html = escape(email_info.html_body)
        subject = email_info.subject
        recipients = [recipient.address for recipient in m.recipients]

        spam_result = await self.spam_service.is_spam_email(prompt_template)
        if spam_result.spam:
            await self.cache_service.set(m, spam_result.reason)
            logger.error(
                "Email flagged as spam",
                extra={
                    "reason": spam_result.reason,
                    "number of recipients": len(m.recipients),
                    "to": recipients,
                    "subject": subject,
                    "company": company_name,
                    "html_escaped": escaped_html,
                },
            )
        return spam_result
