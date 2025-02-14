import re
import time
import logging

from typing import Generator, Tuple
from contextlib import contextmanager

from vocab.util.xml import write_xml, read_xml
from vocab.util.redis import get_object_redis, store_object_redis, delete_object_redis

log = logging.getLogger(__name__)


@contextmanager
def run_work_for_file(file: str) -> Generator[Tuple[int, int], None, None]:
    nr = int(re.search(r'record-(\d+)\.xml', file).group(1))
    id = int(time.time())

    with open(file, 'rb') as f:
        data = f.read()
        store_object_redis(nr, id, data)

    log.info(f"Start work for {file} with nr {nr} and id {id}")
    yield nr, id

    xml = read_xml(get_object_redis(nr, id))
    with open(file, 'wb') as f:
        f.write(write_xml(xml, True))

    delete_object_redis(nr, id)
    log.info(f"Finished work for {file} with nr {nr} and id {id}")
