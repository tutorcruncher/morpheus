from dataclasses import dataclass

from src.render.main import MessageDef, render_email
from src.schemas.messages import EmailSendModel


@dataclass
class LLMPromptTemplate:
    m: EmailSendModel
    instruction_template: str = (
        "You are a spam classification system integrated into a tutoring management platform. "
        "You will be shown an email in XML format. Each email relates to real interactions in the platform, such as: "
        "lesson scheduling, payment requests, welcome/onboarding, admin notifications, or client-tutor communication. "
        "Your job is to analyze the **email metadata and content** to decide whether this email is spam. "
        "Use the following rules to classify the email: "
        "- **SPAM** if the email is irrelevant to the recipient, deceptive, mass marketing, or unsolicited in context. "
        "- **NOT_SPAM** if the email clearly relates to platform activities like bookings, payments, reports, password "
        "resets, or admin workflows. "
    )
    prompt_template: str = (
        "<email>\n"
        "  <subject>{subject}</subject>\n"
        "  <company_name>{company_name}</company_name>\n"
        "  <recipient_name>{full_name}</recipient_name>\n"
        "  <headers>{headers}</headers>\n"
        "  <body><![CDATA[\n{html_body}\n  ]]></body>\n"
        "</email>\n"
    )

    def render_sys_prompt(self) -> str:
        return self.instruction_template

    def render_prompt(self) -> str:
        recipient = self.m.recipients[0] if self.m.recipients else None
        company_name = self.m.context.get("company_name", "")

        # Merge context and headers like the worker
        context = dict(self.m.context, **(recipient.context if recipient and hasattr(recipient, "context") else {}))
        headers = dict(self.m.headers, **(recipient.headers if recipient and hasattr(recipient, "headers") else {}))

        # Build MessageDef for rendering
        message_def = MessageDef(
            first_name=recipient.first_name if recipient else "",
            last_name=recipient.last_name if recipient else "",
            main_template=self.m.main_template,
            mustache_partials=self.m.mustache_partials or {},
            macros=self.m.macros or {},
            subject_template=self.m.subject_template,
            context=context,
            headers=headers,
        )

        email_info = render_email(message_def)
        return self.prompt_template.format(
            subject=email_info.subject,
            company_name=company_name,
            full_name=email_info.full_name,
            headers=email_info.headers,
            html_body=email_info.html_body,
        )
