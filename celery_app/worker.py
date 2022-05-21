from celery import Celery
from dotenv import dotenv_values

config = dotenv_values(".env")

app = Celery(
    'xv_inference',
    broker=config.get('CELERY_BROKER_URI'),
    include=['celery_app.tasks']
)
