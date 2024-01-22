import logging
from itertools import chain

from pylode import OntPub, PylodeError
from rdflib import OWL, RDF, URIRef, DCTERMS, Literal, PROF, SKOS

from vocab.util import load_remote_graph
from vocab.cmdi import get_record, write_location
from vocab.config import vocab_registry_url, docs_path

log = logging.getLogger(__name__)


def get_file_for_id(id):
    return docs_path + id + '.html'


def create_documentation(id):
    def documentation_for_location(location, title):
        try:
            file = get_file_for_id(id)
            graph = load_remote_graph(location['location'])

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
                    graph.add((URIRef(location['location']), RDF.type, OWL.Ontology))
                    graph.add((URIRef(location['location']), DCTERMS.title, Literal(title)))

                od = OntPub(ontology=graph)

            od.make_html(destination=file)

            uri = vocab_registry_url + '/doc/' + id
            write_location(id, record['versions'][0]['version'], uri, 'homepage', 'doc')
            log.info(f'Produced documentation for {id}!')

            return True
        except Exception as e:
            log.error(f'Doc error for {id}: {e}')
            return False

    record = get_record(id)
    if record:
        if 'versions' in record and record['versions'] and 'locations' in record['versions'][0]:
            created_documentation = False
            locations = record['versions'][0]['locations']

            location = next(filter(lambda l: l['type'] == 'endpoint' and l['recipe'] is None, locations), None)
            if location:
                created_documentation = documentation_for_location(location, record['title'])

            if not created_documentation:
                location = next(filter(lambda l: l['type'] == 'endpoint' and l['recipe'] == 'cache', locations), None)
                if location:
                    created_documentation = documentation_for_location(location, record['title'])

            if not location:
                log.info(f'No endpoint found for {id}!')
            elif not created_documentation:
                log.info(f'No documentation for {id}!')
        else:
            log.info(f'No version found for {id}!')
    else:
        log.info(f'No record found for {id}!')
