web: uvicorn app.main:app --host 0.0.0.0 --port $PORT
worker: celery -A app.worker worker --loglevel=info
beat: celery -A app.worker beat --loglevel=info
