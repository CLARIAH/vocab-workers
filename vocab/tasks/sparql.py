import logging
import urllib.parse

from rdflib import Graph

from vocab.app import celery
from vocab.config import sparql_url
from vocab.cmdi import with_version_and_dump, write_location
from vocab.util.rdf import sparql_store, get_vocab_graph_uri, load_cached_into_graph

log = logging.getLogger(__name__)


@celery.task
def load_into_sparql_store(id: str) -> None:
    for record, version, cached_version_path in with_version_and_dump(id):
        try:
            load_into_sparql_store_for_file(id, version.version, cached_version_path)
        except Exception as e:
            log.error(f'Failed to load data into SPARQL for {id} and version {version.version}: {e}')


def load_into_sparql_store_for_file(id: str, version: str, cached_version_path: str) -> None:
    graph_uri = get_vocab_graph_uri(id, version)
    graph = Graph(store=sparql_store, identifier=graph_uri)

    graph_exists = graph.query('ASK WHERE { ?s ?p ?o }')
    if not graph_exists:
        log.info(f"No data found in SPARQL store for {id} with version {version}, creating!")
        load_cached_into_graph(graph, cached_version_path)

        uri = f'{sparql_url}?default-graph-uri={urllib.parse.quote(graph_uri)}'
        write_location(id, version, uri, 'endpoint', 'sparql')

        log.info(f"Data loaded in SPARQL store for {id} with version {version}!")
