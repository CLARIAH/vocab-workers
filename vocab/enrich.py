import os
import logging
import requests

from vocab.util import session
from vocab.util import get_record

log = logging.getLogger(__name__)
url = os.environ.get('SUMMARIZER_URL', 'https://uridid.vocabs.dev.clariah.nl/summarizer')


def lov(id):
    response = session.get('https://lov.linkeddata.es/dataset/lov/api/v2/vocabulary/info', params={'vocab': id})
    if response.status_code == requests.codes.ok:
        data = response.json()
        log.info(f'Work wit vocab {id} results: {data}')
    else:
        log.info(f'No vocab {id} results!')


def summarizer(id):
    record = get_record(id)
    if record:
        response = session.get(url, params={'url': record['endpoint']})
        if response.status_code == requests.codes.ok:
            data = response.json()
            log.info(f'Work with summarizer results for {id}: {data}')
        else:
            log.info(f'No summarizer results for {id}!')
    else:
        log.info(f'No record found for {id}!')
