import sys
import logging

from vocab.app import celery
from vocab.cmdi import get_record
from vocab.config import elasticsearch_index
from vocab.util.elasticsearch import es
from vocab.util.work import get_files_in_path, run_work_for_file

log = logging.getLogger(__name__)


@celery.task(name='index', autoretry_for=(Exception,),
             default_retry_delay=60 * 30, retry_kwargs={'max_retries': 5})
def index(nr: int, id: int) -> None:
    record = get_record(nr, id)
    es.index(
        index=elasticsearch_index,
        id=record.identifier,
        document={
            'id': record.identifier,
            'title': record.title,
            'description': record.description,
            'syntax': record.type.syntax,
            'kos': record.type.kos,
            'entity': record.type.entity,
            'nwo': record.topic.nwo if record.topic is not None else None,
            'unesco': record.topic.unesco if record.topic is not None else None,
            'registries': [registry.title for registry in record.registries]
        })


if __name__ == '__main__':
    for f in get_files_in_path(sys.argv[1]):
        with run_work_for_file(f) as (nr, id):
            index(nr, id)
