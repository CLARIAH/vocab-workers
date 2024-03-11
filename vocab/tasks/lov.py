import logging
import requests

from pydantic import BaseModel

from vocab.app import celery
from vocab.util.http import session
from vocab.cmdi import write_summary_namespace

log = logging.getLogger(__name__)
lov_api_url = 'https://lov.linkeddata.es/dataset/lov/api/v2/vocabulary/info'


class MinimumVocabInfoLOV(BaseModel):
    homepage: str
    nsp: str
    prefix: str
    uri: str


@celery.task
def lov(id):
    response = session.get(lov_api_url, params={'vocab': id})
    if response.status_code == requests.codes.ok:
        data = MinimumVocabInfoLOV.model_validate_json(response.json())
        log.info(f'Work wit vocab {id} results: {data}')

        if data.uri and data.prefix:
            write_summary_namespace(id, data.uri, data.prefix)
    else:
        log.info(f'No vocab {id} results!')
