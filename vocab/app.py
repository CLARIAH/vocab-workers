import os

from celery import Celery

app = Celery('vocab', broker=os.environ.get('REDIS_URI', 'redis://localhost/0'), include=['tasks'])

if __name__ == '__main__':
    argv = [
        'worker',
        '--loglevel=' + os.environ.get('LOG_LEVEL', 'INFO'),
    ]
    app.worker_main(argv)
