import os
import sys
import gzip
import logging

from itertools import chain
from pylode import OntPub, PylodeError
from rdflib import OWL, RDF, URIRef, DCTERMS, Literal, PROF, SKOS, Graph

from vocab.app import celery
from vocab.cmdi import with_version_and_dump, write_location
from vocab.config import vocab_static_url, root_path, docs_rel_path
from vocab.util.file import get_files_in_path, run_work_for_file
from vocab.util.rdf import load_cached_into_graph

log = logging.getLogger(__name__)


def get_relative_path_for_file(id: str, version: str, without_gz: bool = False) -> str:
    return os.path.join(id, version + '.html' + ('' if without_gz else '.gz'))


@celery.task(name='rdf.documentation', autoretry_for=(Exception,),
             default_retry_delay=60 * 30, retry_kwargs={'max_retries': 5})
def create_documentation(nr: int, id: int):
    for record, version, cached_version_path in with_version_and_dump(nr, id):
        if record.type.syntax in ['owl', 'skos', 'rdfs']:
            path = os.path.join(root_path, docs_rel_path,
                                get_relative_path_for_file(record.identifier, version.version))
            if not os.path.exists(path):
                log.info(f"No documentation found for {record.identifier} with version {version.version}, creating!")
                location = next((loc for loc in version.locations if loc.type == 'dump'), None)
                create_documentation_for_file(nr, id, record.identifier, version.version, record.title,
                                              location.location,
                                              cached_version_path)
            else:
                log.info(f"Write documentation location for {record.identifier} and version {version.version}")
                write_docs_location(nr, id, record.identifier, version.version)


def create_documentation_for_file(nr: int, id: int, identifier: str, version: str, title: str, uri: str,
                                  cached_version_path: str) -> None:
    try:
        graph = Graph()
        load_cached_into_graph(graph, cached_version_path)

        try:
            od = OntPub(ontology=graph)
        except PylodeError:
            subjects = chain(
                graph.subjects(RDF.type, OWL.Ontology),
                graph.subjects(RDF.type, PROF.Profile),
                graph.subjects(RDF.type, SKOS.ConceptScheme),
            )

            if subjects:
                for s in subjects:
                    graph.add((s, DCTERMS.title, Literal(title)))
            else:
                graph.add((URIRef(uri), RDF.type, OWL.Ontology))
                graph.add((URIRef(uri), DCTERMS.title, Literal(title)))

            od = OntPub(ontology=graph)

        doc_path = os.path.join(root_path, docs_rel_path, get_relative_path_for_file(identifier, version))
        os.makedirs(os.path.dirname(doc_path), exist_ok=True)

        html = od.make_html()
        html = gzip.compress(bytes(html, 'utf-8'))
        open(doc_path, 'wb').write(html)

        uri = vocab_static_url + '/docs/' + get_relative_path_for_file(identifier, version, without_gz=True)
        write_location(nr, id, version, uri, 'homepage', 'doc')
        log.info(f'Produced documentation for {identifier} with version {version}!')
    except Exception as e:
        log.error(f'Doc error for {identifier} with version {version}: {e}')


def write_docs_location(nr: int, id: int, identifier: str, version: str) -> None:
    uri = vocab_static_url + '/docs/' + get_relative_path_for_file(identifier, version, without_gz=True)
    write_location(nr, id, version, uri, 'homepage', 'doc')


if __name__ == '__main__':
    for f in get_files_in_path(sys.argv[1]):
        with run_work_for_file(f) as (nr, id):
            create_documentation(nr, id)
