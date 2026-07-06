from celery import Celery
from celery.schedules import crontab

from app.core.config import settings

celery_app = Celery(
    'morpheus',
    broker=settings.redis_url,
    backend=settings.redis_url,
    include=['app.messages.tasks'],
)

celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    broker_connection_retry_on_startup=True,
)


celery_app.conf.beat_schedule = {
    'update-aggregation-view': {
        'task': 'app.messages.tasks.update_aggregation_view',
        'schedule': crontab(minute='12'),
    },
    'delete-old-emails': {
        'task': 'app.messages.tasks.delete_old_emails',
        'schedule': crontab(minute='30'),
    },
}
