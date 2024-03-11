import logging
import requests

from vocab.app import celery
from vocab.util.http import session
from vocab.config import summarizer_url
from vocab.cmdi import get_record, write_summary_statements

log = logging.getLogger(__name__)


@celery.task
def summarizer(id: str) -> None:
    record = get_record(id)
    if record:
        if record.versions and record.versions[0].locations:
            wrote_summary = False
            locations = record.versions[0].locations

            location = next(filter(lambda l: l.type == 'endpoint' and l.recipe is None, locations), None)
            if location:
                wrote_summary = summarizer_for_uri(location.location)

            if not wrote_summary:
                location = next(filter(lambda l: l.type == 'endpoint' and l.recipe == 'cache', locations), None)
                if location:
                    wrote_summary = summarizer_for_uri(location.location)

            if not location:
                log.info(f'No endpoint found for {id}!')
            elif not wrote_summary:
                log.info(f'No summarizer results for {id}!')
        else:
            log.info(f'No version found for {id}!')
    else:
        log.info(f'No record found for {id}!')


def summarizer_for_uri(uri: str) -> bool:
    response = session.get(summarizer_url, params={'url': uri})
    if response.status_code == requests.codes.ok:
        data = response.json()
        log.info(f'Work with summarizer results for {id}: {data}')

        write_summary_statements(id, data)
        log.info(f'Wrote summarizer results for {id}: {data}')

        return True
    else:
        return False
