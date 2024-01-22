from celery import Celery

from vocab.config import redis_uri, log_level, concurrency

app = Celery('vocab', broker=redis_uri, backend=redis_uri, include=['tasks'])

if __name__ == '__main__':
    argv = [
        'worker',
        '--loglevel=' + log_level,
        '--concurrency=' + str(concurrency)
    ]
    app.worker_main(argv)
