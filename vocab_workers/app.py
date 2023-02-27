import os
import uuid
import stomp
import logging.config

from vocab_workers.workers.Listener import Listener
from vocab_workers.workers.SummarizerWorker import SummarizerWorker
from vocab_workers.workers.LinkedOpenVocabulariesWorker import LinkedOpenVocabulariesWorker

logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'default': {
            'format': '%(asctime)s - %(levelname)s - %(name)s: %(message)s'
        },
    },
    'handlers': {
        'default': {
            'class': 'logging.StreamHandler',
            'formatter': 'default'
        },
    },
    'root': {
        'handlers': ['default'],
        'level': os.environ.get('LOG_LEVEL', 'WARN'),
        'propagate': True
    }
})

conn = stomp.Connection([(os.environ.get('MQ_HOST', 'localhost'), int(os.environ.get('MQ_PORT', 61613)))])
conn.connect(os.environ.get('MQ_USER', 'system'), os.environ.get('MQ_PASSWORD', 'manager'), wait=True)
conn.subscribe(destination=os.environ.get('MQ_DESTINATION', '/vocabs'), id=str(uuid.uuid4()), ack='client')

conn.set_listener('', Listener(conn, [
    LinkedOpenVocabulariesWorker(),
    SummarizerWorker()
]))

while True:
    pass
