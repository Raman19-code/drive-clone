"""
Celery application wiring for the DriveX ML pipeline.

Tasks (embed_file, generate_tags, dedup_check) are triggered by
upload-complete events published to RabbitMQ by the C++ API, and never
sit in the request/response path of an upload.
"""
import os
from celery import Celery

BROKER_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672//")
RESULT_BACKEND = os.getenv("REDIS_URL", "redis://localhost:6379/0")

celery_app = Celery(
    "drivex_ml",
    broker=BROKER_URL,
    backend=RESULT_BACKEND,
    include=[
        "app.tasks.embed_file",
        "app.tasks.generate_tags",
        "app.tasks.dedup_check",
    ],
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    task_acks_late=True,
    worker_prefetch_multiplier=1,
)
