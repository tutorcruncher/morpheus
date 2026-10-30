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
    # Copies to the mirror database (issue #502) run on their own queue, served by the Procfile's
    # mirror process, so a slow or unreachable mirror never holds up sends.
    task_routes={'app.messages.mirror.*': {'queue': 'mirror'}},
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
    # hourly would be an hourly Sentry issue forever. The hour itself barely matters: TutorCruncher
    # posts one delete per agency as its 3-hourly job works through them, so at most a purge or two
    # is ever in flight, and a sweep that does meet one blocks on it and then deletes nothing.
    'purge-deleted-companies': {
        'task': 'app.messages.tasks.purge_deleted_companies',
        'schedule': crontab(hour='5', minute='45'),
    },
}

# Mirror database (issue #502): re-copies companies whose purge or rename never reached it. Only
# scheduled with MIRROR_DATABASE_URL set: it runs on the mirror queue, which has no consumer before
# that, so every firing would wait in Redis.
if settings.mirror_database_url:
    celery_app.conf.beat_schedule['reconcile-mirror-companies'] = {
        'task': 'app.messages.mirror.reconcile_mirror_companies',
        'schedule': crontab(minute='50'),
    }
