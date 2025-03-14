import os
import sys
import logging

from rdflib import Graph, Namespace, DC, VOID, RDF, Literal, URIRef

from vocab.app import celery
from vocab.cmdi import with_version, write_location
from vocab.config import root_path, vocab_registry_url, skosmos_url
from vocab.util.lock import task_lock
from vocab.util.work import get_files_in_path, run_work_for_file

log = logging.getLogger(__name__)

LOCAL = Namespace('#')
SKOSMOS = Namespace('http://purl.org/net/skosmos#')


@celery.task(name='rdf.skosmos')
def add_to_skosmos_config(nr: int, id: int):
    for record, version in with_version(nr, id):
        if record.type.syntax == 'skos':
            log.info(f'Create Skosmos config for {record.identifier} and version {version.version}')
            update_skosmos_config_with(nr, id, record.identifier, version.version, record.title)


@task_lock(main_key="update_skosmos_config")
def update_skosmos_config_with(nr: int, id: int, identifier: str, version, title):
    config_file_path = os.path.join(root_path, 'config.ttl')

    graph = Graph()
    graph.parse(config_file_path)

    uri = LOCAL[identifier + '__' + version]
    if uri not in graph.subjects():
        vocab_config_graph = create_skosmos_vocab_config(uri, identifier, version, title)
        graph = graph + vocab_config_graph

        config_ttl = graph.serialize(format='ttl')
        with open(config_file_path, 'w') as f:
            f.write(config_ttl)

        write_location(nr, id, version, f'{skosmos_url}/{identifier}__{version}', 'homepage', 'skosmos')


def create_skosmos_vocab_config(uri: URIRef, identifier: str, version: str, title: str) -> Graph:
    graph_uri = URIRef(f'{vocab_registry_url}/vocab/{identifier}/version/{version}')

    graph = Graph()

    graph.bind('dc', DC)
    graph.bind('void', VOID)
    graph.bind('skosmos', SKOSMOS)

    graph.add((uri, RDF.type, SKOSMOS.Vocabulary))
    graph.add((uri, DC.title, Literal(title + ' @ ' + version, lang='en')))
    graph.add((uri, SKOSMOS.shortName, Literal(id + '@' + version)))
    graph.add((uri, SKOSMOS.language, Literal('en')))
    graph.add((uri, VOID.uriSpace, graph_uri))
    graph.add((uri, SKOSMOS.sparqlGraph, graph_uri))

    return graph


if __name__ == '__main__':
    for f in get_files_in_path(sys.argv[1]):
        with run_work_for_file(f) as (nr, id):
            add_to_skosmos_config(nr, id)
