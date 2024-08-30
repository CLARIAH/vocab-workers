import gzip
import xml.sax

from rdflib import Graph, BNode, URIRef
from rdflib.term import Node
from rdflib.util import guess_format
from rdflib.parser import Parser
from rdflib.graph import BatchAddGraph
from rdflib.exceptions import ParserError
from rdflib.plugin import PluginException, register
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore, _node_to_sparql

from vocab.config import sparql_url, sparql_update_url

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


def encode_bnode_to_sparql(node: Node | str) -> str:
    if isinstance(node, BNode):
        return '_:b%s' % node
    return _node_to_sparql(node)


def get_sparql_store(context_aware: bool = True) -> SPARQLUpdateStore:
    return SPARQLUpdateStore(query_endpoint=sparql_url, update_endpoint=sparql_update_url,
                             node_to_sparql=encode_bnode_to_sparql, context_aware=context_aware)


def load_cached_into_graph(graph: Graph, cached_version_path: str, use_batch: bool = False) -> None:
    memory_graph = Graph() if use_batch else None

    try:
        with gzip.open(cached_version_path, 'r') as vocab_data:
            format = guess_format(cached_version_path[:-3])
            format = format if format is not None else 'xml'
            (memory_graph if use_batch else graph).parse(vocab_data, format=format)
    except xml.sax._exceptions.SAXParseException:
        with gzip.open(cached_version_path, 'r') as vocab_data:
            (memory_graph if use_batch else graph).parse(vocab_data, format='ttl')

    if use_batch:
        with BatchAddGraph(graph, batch_size=100) as batch:
            for triple in memory_graph:
                batch.add(triple)


def load_remote_graph(url: str) -> Graph:
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
