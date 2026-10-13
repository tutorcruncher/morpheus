web: uvicorn app.main:app --host 0.0.0.0 --port $PORT
worker: celery -A app.worker worker --loglevel=info
beat: celery -A app.worker beat --loglevel=info
mirror: celery -A app.worker worker -Q mirror --concurrency 2 --loglevel=info
