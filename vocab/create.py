import os
import logging

from rdflib import Graph
from rdflib.plugin import register, Parser
from pylode import OntPub

from cmdi import get_record, write_location

log = logging.getLogger(__name__)
vocab_registry_url = os.environ.get('VOCAB_REGISTRY_URL', 'https://localhost:5000')

register('application/owl+xml', Parser, 'rdflib.plugins.parsers.rdfxml', 'RDFXMLParser')


def get_file_for_id(id):
    return os.environ.get('DOCS_PATH', '../data/docs/') + id + '.html'


def create_documentation(id):
    def documentation_for_location(location):
        try:
            file = get_file_for_id(id)

            graph = Graph().parse(location=location['location'])
            od = OntPub(ontology=graph)
            od.make_html(destination=file)

            uri = vocab_registry_url + '/doc/' + id
            write_location(id, record['versions'][0]['version'], uri, 'homepage', 'doc')
            log.info(f'Produced documentation for {id}!')

            return True
        except Exception as e:
            return False

    record = get_record(id)
    if record:
        if 'versions' in record and record['versions'] and 'locations' in record['versions'][0]:
            created_documentation = False
            locations = record['versions'][0]['locations']

            location = next(filter(lambda l: l['type'] == 'endpoint' and l['recipe'] is None, locations), None)
            if location:
                created_documentation = documentation_for_location(location)

            if not created_documentation:
                location = next(filter(lambda l: l['type'] == 'endpoint' and l['recipe'] == 'cache', locations), None)
                if location:
                    created_documentation = documentation_for_location(location)

            if not location:
                log.info(f'No endpoint found for {id}!')
            elif not created_documentation:
                log.info(f'No documentation for {id}!')
        else:
            log.info(f'No version found for {id}!')
    else:
        log.info(f'No record found for {id}!')
