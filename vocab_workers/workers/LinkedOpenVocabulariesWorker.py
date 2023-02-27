import logging
import requests

from vocab_workers.workers.Worker import Worker

log = logging.getLogger(__name__)


class LinkedOpenVocabulariesWorker(Worker):
    def run(self, id):
        response = self._session.get('https://lov.linkeddata.es/dataset/lov/api/v2/vocabulary/info',
                                     params={'vocab': id})
        if response.status_code == requests.codes.ok:
            data = response.json()
            log.info(f'Work wit vocab {id} results: {data}')
        else:
            log.info(f'No vocab {id} results!')
