import os
import re
import time
import logging
import requests

from typing import Generator, Tuple
from contextlib import contextmanager

from vocab.util.xml import write_xml, read_xml
from vocab.util.redis import get_object_redis, store_object_redis, delete_object_redis

from pydantic import BaseModel
from vocab.app import celery
from vocab.util.http import session

log = logging.getLogger(__name__)

editor_api_record = 'http://localhost:1210'

@contextmanager
def run_work_for_file(file: str) -> Generator[Tuple[int, int], None, None]:
    nr = int(re.search(r'record-(\d+)\.xml', file).group(1))
    id = int(time.time())

    with open(file, 'rb') as f:
        data = f.read()
        store_object_redis(nr, id, data)

    log.info(f"Start work for {file} with nr {nr} and id {id}")
    yield nr, id

    log.info(f"Store xml for {file} with nr {nr} and id {id}")

    xml = read_xml(get_object_redis(nr, id))
    with open(file, 'wb') as f:
        f.write(write_xml(xml, True))

    delete_object_redis(nr, id)
    log.info(f"Finished work for {file} with nr {nr} and id {id}")

@contextmanager
def run_work_for_record(nr: int) -> Generator[int, None, None]:
    id = int(time.time())
#   record = requests 'http://localhost:1210/app/vocabs/profile/clarin.eu%3Acr1%3Ap_1653377925723/record/{nr}' ## here we will call the API with a Get request to the Editor, only add the number
#   Figure out how to change the accept header with request (to xml instead of json)
    response = session.get(f"{editor_api_record}/app/vocabs/profile/clarin.eu%3Acr1%3Ap_1653377925723/record/{nr}",
                         headers={"accept":"application/xml"})

    store_object_redis(nr, id, response.content)

    log.info(f"Start work for {nr} with id {id}")
    yield id

    log.info(f"Store xml for {nr} with id {id}")

    xml = read_xml(get_object_redis(nr, id))
    # here comes PUT request to editor (/app/{app}/profile/{prof}/record/{nr})
    update_record = session.put(f"{editor_api_record}/app/vocabs/profile/clarin.eu%3Acr1%3Ap_1653377925723/record/{nr}",
                                body=xml, headers={"content-type":"application/xml"})

    delete_object_redis(nr, id)
    log.info(f"Finished work for {nr} with id {id}")

def get_files_in_path(path: str) -> [str]:
    if os.path.isfile(path):
        return [path]
    else:
        for (dirpath, dirnames, filenames) in os.walk(path):
            if dirpath == path:
                return [os.path.join(dirpath, f) for f in filenames]
