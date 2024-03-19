import os
import logging

from rdflib import Graph, Namespace, DC, VOID, RDF, Literal, URIRef

from vocab.app import celery
from vocab.cmdi import with_version, write_location
from vocab.config import skosmos_config_path
from vocab.util.lock import task_lock

log = logging.getLogger(__name__)

LOCAL = Namespace('#')
SKOSMOS = Namespace('http://purl.org/net/skosmos#')


@celery.task(name='rdf.skosmos')
def add_to_skosmos_config(id: str):
    for record, version in with_version(id):
        # TODO: if record.type == 'skos':
        log.info(f'Create skosmos config for {id} and version {version.version}')
        update_skosmos_config_with(id, version.version, record.title)


@task_lock(main_key="update_skosmos_config")
def update_skosmos_config_with(id, version, title):
    config_file_path = os.path.join(skosmos_config_path, 'config.ttl')

    graph = Graph()
    graph.parse(config_file_path)

    vocab_config_graph = create_skosmos_vocab_config(id, version, title)
    graph = graph + vocab_config_graph

    config_ttl = graph.serialize(format='ttl')
    with open(config_file_path, 'w') as f:
        f.write(config_ttl)

    write_location(id, version, f'https://skosmos.vocabs.dev.clariah.nl/{id}', 'homepage', 'skosmos')


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
