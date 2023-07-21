import os
import logging
import requests

from vocab.util import session
from vocab.cmdi import get_record, write_summary, write_location

log = logging.getLogger(__name__)
url = os.environ.get('SUMMARIZER_URL', 'https://api.zandbak.dans.knaw.nl/summarizer')


def lov(id):
    response = session.get('https://lov.linkeddata.es/dataset/lov/api/v2/vocabulary/info', params={'vocab': id})
    if response.status_code == requests.codes.ok:
        data = response.json()
        log.info(f'Work wit vocab {id} results: {data}')
    else:
        log.info(f'No vocab {id} results!')


def summarizer(id):
    def summarizer_for_location(location):
        response = session.get(url, params={'url': location['location']})
        if response.status_code == requests.codes.ok:
            data = response.json()
            log.info(f'Work with summarizer results for {id}: {data}')

            write_summary(id, data)
            log.info(f'Wrote summarizer results for {id}: {data}')

            return True
        else:
            return False

    record = get_record(id)
    if record:
        if 'versions' in record and record['versions'] and 'locations' in record['versions'][0]:
            wrote_summary = False
            locations = record['versions'][0]['locations']

            location = next(filter(lambda l: l['type'] == 'endpoint' and l['recipe'] is None, locations), None)
            if location:
                wrote_summary = summarizer_for_location(location)

            if not wrote_summary:
                location = next(filter(lambda l: l['type'] == 'endpoint' and l['recipe'] == 'cache', locations), None)
                if location:
                    wrote_summary = summarizer_for_location(location)

            if not location:
                log.info(f'No endpoint found for {id}!')
            elif not wrote_summary:
                log.info(f'No summarizer results for {id}!')
        else:
            log.info(f'No version found for {id}!')
    else:
        log.info(f'No record found for {id}!')


# def skosmos(id):
#     record = get_record(id)
#     if record and record['summary'] and \
#             next(filter(lambda stat: stat['prefix'] == 'skos', record['summary']['stats']), None):
#         log.info(f'SKOS found for {id}')
#
#         write_location(id, version, f'https://skosmos.vocabs.dev.clariah.nl/{id}', 'homepage', 'skosmos')
#         write_location(id, version, f'', 'endpoint', 'sparql')
#
#         log.info(f'Wrote SKOS locations for {id}:')
#     else:
#         log.info(f'No SKOS found for {id}!')
