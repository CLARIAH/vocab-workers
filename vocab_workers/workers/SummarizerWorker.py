import logging
import os

import requests

from vocab_workers.cmdi import get_record
from vocab_workers.workers.Worker import Worker

log = logging.getLogger(__name__)
url = os.environ.get('SUMMARIZER_URL', 'https://uridid.vocabs.dev.clariah.nl/summarizer')


class SummarizerWorker(Worker):
    def run(self, id):
        record = get_record(id)
        if record:
            response = self._session.get(url, params={'url': record['endpoint']})
            if response.status_code == requests.codes.ok:
                data = response.json()
                log.info(f'Work with summarizer results for {id}: {data}')
            else:
                log.info(f'No summarizer results for {id}!')
        else:
            log.info(f'No record found for {id}!')
