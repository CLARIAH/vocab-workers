from celery import chain

from vocab.app import celery
from vocab.util.work import run_work_for_file, run_work_for_record
from vocab.tasks import cache, documentation, sparql, summarizer, lov, skosmos, jsonld, index


def pipeline(nr: int, id: int):
    return chain(
        cache.cache_files.si(nr, id),
        documentation.create_documentation.si(nr, id),
        sparql.load_into_sparql_store.si(nr, id),
        summarizer.summarizer.si(nr, id),
        lov.lov.si(nr, id),
        skosmos.add_to_skosmos_config.si(nr, id),
        jsonld.create_jsonld.si(nr, id),
        index.index.si(nr, id),
    )().get()


def run_pipeline_with_file(file: str):
    with run_work_for_file(file) as (nr, id):
        pipeline(nr, id)


@celery.task(name='pipeline')
def run_pipeline_with_record(nr: int):
    with run_work_for_record(nr) as id:
        pipeline(nr, id)
