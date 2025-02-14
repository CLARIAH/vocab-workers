import os
import sys
import logging

from rdflib import Graph, Namespace, DC, VOID, RDF, Literal, URIRef

from vocab.app import celery
from vocab.cmdi import with_version, write_location
from vocab.config import root_path
from vocab.util.lock import task_lock
from vocab.util.file import run_work_for_file

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

    vocab_config_graph = create_skosmos_vocab_config(identifier, version, title)
    graph = graph + vocab_config_graph

    config_ttl = graph.serialize(format='ttl')
    with open(config_file_path, 'w') as f:
        f.write(config_ttl)

    write_location(nr, id, version, f'https://skosmos.vocabs.dev.clariah.nl/{identifier}', 'homepage', 'skosmos')


def create_skosmos_vocab_config(id: str, version: str, title: str) -> Graph:
    uri = LOCAL[id + '__' + version]

    graph = Graph()

    graph.bind('dc', DC)
    graph.bind('void', VOID)
    graph.bind('skosmos', SKOSMOS)

    graph.add((uri, RDF.type, SKOSMOS.Vocabulary))
    graph.add((uri, DC.title, Literal(title + ' @ ' + version, lang='en')))
    graph.add((uri, SKOSMOS.shortName, Literal(id + '@' + version)))
    graph.add((uri, SKOSMOS.language, Literal('en')))
    graph.add((uri, VOID.uriSpace, Literal("urn:vocab:" + id + '@' + version)))
    graph.add((uri, SKOSMOS.sparqlGraph, URIRef("urn:vocab:" + id + '@' + version)))

    return graph


if __name__ == '__main__':
    with run_work_for_file(sys.argv[1]) as (nr, id):
        add_to_skosmos_config(nr, id)
