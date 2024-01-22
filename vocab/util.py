import requests

from rdflib import Graph
from rdflib.exceptions import ParserError
from rdflib.parser import Parser
from rdflib.plugin import PluginException, register
from rdflib.util import guess_format
from requests.adapters import HTTPAdapter, Retry

session = requests.Session()
session.mount('https://', HTTPAdapter(max_retries=Retry(total=10, backoff_factor=1)))

register('rdfs', Parser, 'rdflib.plugins.parsers.rdfxml', 'RDFXMLParser')
register('owl', Parser, 'rdflib.plugins.parsers.rdfxml', 'RDFXMLParser')
register('application/owl+xml', Parser, 'rdflib.plugins.parsers.rdfxml', 'RDFXMLParser')

content_type_extensions = {
    'application/owl+xml': '.owl',
    'application/rdf+xml': '.rdf',
    'text/n3': '.n3',
    'text/turtle': '.ttl',
    'application/n-triples': '.nt',
    'application/ld+json': '.jsonld',
    'application/n-quads': '.nq',
    'application/trix': '.trix',
    'application/trig': '.trig',
    'text/trig': 'trig'
}


def load_remote_graph(url):
    try:
        return Graph().parse(location=url)
    except (PluginException, ParserError):
        try:
            return Graph().parse(location=url, format=guess_format(url))
        except (PluginException, ParserError):
            try:
                return Graph().parse(location=url, format='xml')
            except (PluginException, ParserError):
                return Graph().parse(location=url, format='ttl')
