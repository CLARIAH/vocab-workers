import sys
import logging
import urllib.parse

from rdflib import Graph, URIRef

from vocab.app import celery
from vocab.config import sparql_url, vocab_registry_url
from vocab.cmdi import with_version_and_dump, write_location
from vocab.util.file import run_work_for_file
from vocab.util.rdf import get_sparql_store, load_cached_into_graph

log = logging.getLogger(__name__)


@celery.task(name='rdf.sparql', autoretry_for=(Exception,),
             default_retry_delay=60 * 30, retry_kwargs={'max_retries': 5})
def load_into_sparql_store(nr: int, id: int) -> None:
    for record, version, cached_version_path in with_version_and_dump(nr, id):
        if record.type.syntax in ['owl', 'skos', 'rdfs']:
            try:
                load_into_sparql_store_for_file(nr, id, record.identifier, version.version, cached_version_path)
            except Exception as e:
                log.error(f'Failed to load data into SPARQL for {record.identifier} and version {version.version}: {e}')


def load_into_sparql_store_for_file(nr: int, id: int, identifier: str, version: str, cached_version_path: str) -> None:
    graph_uri = URIRef(f'{vocab_registry_url}/vocab/{identifier}/version/{version}')
    graph = Graph(store=get_sparql_store(True), identifier=graph_uri)

    graph_exists = graph.query('ASK WHERE { ?s ?p ?o }')
    if not graph_exists:
        log.info(f"No data found in SPARQL store for {identifier} with version {version}, creating!")
        load_cached_into_graph(graph, cached_version_path)

        uri = f'{sparql_url}?default-graph-uri={urllib.parse.quote(graph_uri)}'
        write_location(nr, id, version, uri, 'endpoint', 'sparql')

        log.info(f"Data loaded in SPARQL store for {identifier} with version {version}!")


if __name__ == '__main__':
    with run_work_for_file(sys.argv[1]) as (nr, id):
        load_into_sparql_store(nr, id)
