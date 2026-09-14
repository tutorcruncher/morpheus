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
    # Daily, not hourly: this only catches a purge lost to a broker blip or a worker still on the
    # previous release, nothing waits on the swept rows, and a permanently failing purge re-queued
    # hourly would be an hourly Sentry issue forever. 05:45 sits in the widest gap between
    # TutorCruncher's 3-hourly terminated-agency runs, so the sweep rarely meets a purge still going.
    'purge-deleted-companies': {
        'task': 'app.messages.tasks.purge_deleted_companies',
        'schedule': crontab(hour='5', minute='45'),
    },
}
