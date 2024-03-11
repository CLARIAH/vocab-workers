import os
import logging

from itertools import chain
from pylode import OntPub, PylodeError
from rdflib import OWL, RDF, URIRef, DCTERMS, Literal, PROF, SKOS, Graph

from vocab.app import celery
from vocab.util.rdf import load_cached_into_graph
from vocab.cmdi import with_version_and_dump, write_location
from vocab.config import vocab_registry_url, docs_path

log = logging.getLogger(__name__)


def get_doc_path(id: str, version: str) -> str:
    return os.path.join(docs_path, id, version + '.html')


@celery.task
def create_documentation(id: str):
    for record, version, cached_version_path in with_version_and_dump(id):
        if not os.path.exists(get_doc_path(id, version.version)):
            log.info(f"No documentation found for {id} with version {version.version}, creating!")
            location = next((loc for loc in version.locations if loc.type == 'endpoint'), None)
            create_documentation_for_file(id, version.version, record.title, location.location, cached_version_path)


def create_documentation_for_file(id: str, version: str, title: str, uri: str, cached_version_path: str) -> None:
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

        doc_path = get_doc_path(id, version)
        os.makedirs(os.path.dirname(doc_path), exist_ok=True)
        od.make_html(destination=doc_path)

        uri = vocab_registry_url + '/doc/' + id
        write_location(id, version, uri, 'homepage', 'doc')
        log.info(f'Produced documentation for {id} with version {version}!')
    except Exception as e:
        log.error(f'Doc error for {id} with version {version}: {e}')
