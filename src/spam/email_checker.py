import logging
from html import escape

from src.render.main import MessageDef, render_email
from src.schemas.messages import EmailSendModel
from src.spam.services import OpenAISpamEmailService, SpamCacheService

logger = logging.getLogger('spam.email_checker')


class EmailSpamChecker:
    def __init__(self, spam_service: OpenAISpamEmailService, cache_service: SpamCacheService):
        self.spam_service = spam_service
        self.cache_service = cache_service

    async def check_spam(self, m: EmailSendModel):
        """
        Check if an email is spam using cached results or AI service.

        First checks cache for existing spam result. If not found, renders the email,
        sends it to the AI spam detection service, caches the result, and logs if spam.
        """
        spam_result = await self.cache_service.get(m)
        if spam_result:
            return spam_result

        # prepare email info for spam check for the first recipient email only
        recipient = m.recipients[0]
        context = dict(m.context, **recipient.context)
        headers = dict(m.headers, **recipient.headers)
        message_def = MessageDef(
            first_name=recipient.first_name,
            last_name=recipient.last_name,
            main_template=m.main_template,
            mustache_partials=m.mustache_partials or {},
            macros=m.macros or {},
            subject_template=m.subject_template,
            context=context,
            headers=headers,
        )
        email_info = render_email(message_def)
        company_name = m.context.get("company_name", "no_company")
        escaped_html = escape(email_info.html_body)
        subject = email_info.subject
        recipients = [recipient.address for recipient in m.recipients]

        spam_result = await self.spam_service.is_spam_email(email_info, company_name)

        # Cache all results (both spam and non-spam)
        await self.cache_service.set(m, spam_result)

        if spam_result.spam:
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
