import importlib
from loguru import logger
from celery import tasks

from .worker import app

@app.task()
def run_xv(self, data):
    # Run xv2 here
    pass