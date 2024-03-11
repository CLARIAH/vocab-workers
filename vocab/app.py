import sys

from celery import Celery
from vocab.config import redis_uri, log_level, concurrency

celery = Celery(
    'vocab',
    broker=redis_uri,
    backend=redis_uri,
    include=[
        'vocab.tasks.cache',
        'vocab.tasks.documentation',
        'vocab.tasks.lov',
        'vocab.tasks.skosmos',
        'vocab.tasks.sparql',
        'vocab.tasks.summarizer',
    ],
    task_ignore_result=True,
    task_store_errors_even_if_ignored=True,
)

if __name__ == '__main__':
    if sys.argv[1] == 'flower':
        celery.start(['flower'])
    elif sys.argv[1] == 'worker':
        celery.worker_main([
            'worker',
            '--loglevel=' + log_level,
            '--concurrency=' + str(concurrency)
        ])
