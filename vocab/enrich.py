import os
import logging
import requests

from vocab.util import session
from vocab.cmdi import get_record, write_summary, write_location

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
        location = next(filter(lambda loc: loc['type'] == 'endpoint', record['locations']), None)
        if location:
            response = session.get(url, params={'url': location['location']})
            if response.status_code == requests.codes.ok:
                data = response.json()
                log.info(f'Work with summarizer results for {id}: {data}')

                write_summary(id, data)
                log.info(f'Wrote summarizer results for {id}: {data}')
            else:
                log.info(f'No summarizer results for {id}!')
        else:
            log.info(f'No endpoint found for {id}!')
    else:
        log.info(f'No record found for {id}!')


def skosmos(id):
    record = get_record(id)
    if record and record['summary'] and \
            next(filter(lambda stat: stat['prefix'] == 'skos', record['summary']['stats']), None):
        log.info(f'SKOS found for {id}')

        write_location(id, f'https://skosmos.vocabs.dev.clariah.nl/{id}', 'homepage', 'skosmos')
        write_location(id, f'', 'endpoint', 'sparql')

        log.info(f'Wrote SKOS locations for {id}:')
    else:
        log.info(f'No SKOS found for {id}!')
