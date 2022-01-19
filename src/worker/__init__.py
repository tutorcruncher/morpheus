import arq
from arq import cron, run_worker
from foxglove import glove

from src.ext import Mandrill, MessageBird
from src.settings import Settings
from src.worker.email import email_retrying, send_email
from src.worker.scheduler import delete_old_emails, update_aggregation_view
from src.worker.sms import send_sms
from src.worker.webhooks import store_click, update_mandrill_webhooks, update_message_status


async def startup(ctx):
    settings = glove.settings
    glove.redis = ctx.get('redis') or await arq.create_pool(settings.redis_settings)
    ctx.update(
        email_click_url=f'https://{settings.click_host_name}/l',
        sms_click_url=f'{settings.click_host_name}/l',
        mandrill=Mandrill(settings=settings),
        messagebird=MessageBird(settings=settings),
    )
    await glove.startup(run_migrations=False)


async def shutdown(ctx):
    glove.redis = None
    await glove.shutdown()
    if hasattr(glove, 'mandrill'):
        delattr(glove, 'mandrill')


worker_settings = dict(
    job_timeout=60,
    max_jobs=20,
    keep_result=5,
    max_tries=len(email_retrying) + 1,  # so we try all values in email_retrying
    functions=(
        send_email,
        send_sms,
        update_mandrill_webhooks,
        store_click,
        update_message_status,
        update_aggregation_view,
        delete_old_emails,
    ),
    on_startup=startup,
    on_shutdown=shutdown,
    cron_jobs=[
        cron(update_aggregation_view, minute=12, timeout=1800),
        cron(delete_old_emails, minute={30}),
    ],
)


def main(settings: Settings):  # pragma: no cover
    run_worker(worker_settings, redis_settings=settings.redis_settings, ctx={'settings': settings})
