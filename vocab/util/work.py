import os
import re
import time
import logging

from typing import Generator, Tuple
from contextlib import contextmanager

from vocab.config import editor_uri
from vocab.util.http import session
from vocab.util.xml import write_xml, read_xml
from vocab.util.redis import get_object_redis, store_object_redis, delete_object_redis

log = logging.getLogger(__name__)


@contextmanager
def run_work_for_file(file: str) -> Generator[Tuple[int, int], None, None]:
    try:
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
    except Exception as e:
        log.error(f"Error processing file {file}: {e}", exc_info=True)


@contextmanager
def run_work_for_record(nr: int) -> Generator[int, None, None]:
    id = int(time.time())
    response = session.get(f"{editor_uri}/app/vocabs/profile/clarin.eu%3Acr1%3Ap_1653377925723/record/{nr}",
                           headers={"accept": "application/xml"})

    store_object_redis(nr, id, response.content)

    log.info(f"Start work for {nr} with id {id}")
    yield id

    log.info(f"Save XML back to editor for {nr} with id {id}")

    xml = read_xml(get_object_redis(nr, id))
    session.put(f"{editor_uri}/app/vocabs/profile/clarin.eu%3Acr1%3Ap_1653377925723/record/{nr}",
                headers={"content-type": "application/xml"}, body=xml)

    delete_object_redis(nr, id)
    log.info(f"Finished work for {nr} with id {id}")


def get_files_in_path(path: str) -> list[str]:
    if os.path.isfile(path):
        return [path]
    else:
        for (dirpath, dirnames, filenames) in os.walk(path):
            if dirpath == path:
                return [os.path.join(dirpath, f) for f in filenames]
        return []
