import gzip
import xml.sax
import requests

from rdflib import Graph, BNode
from rdflib.term import Node
from rdflib.util import guess_format
from rdflib.parser import Parser
from rdflib.graph import BatchAddGraph
from rdflib.exceptions import ParserError
from rdflib.plugin import PluginException, register
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore, _node_to_sparql

from vocab.config import sparql_url, sparql_update_url, sparql_user, sparql_password

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
                             auth=(sparql_user, sparql_password) if sparql_user else None,
                             node_to_sparql=encode_bnode_to_sparql, context_aware=context_aware)


def load_cached_into_graph(graph: Graph, cached_version_path: str, use_batch: bool = False, format: str = None) -> None:
    memory_graph = Graph() if use_batch else None

    try:
        use_format = format
        if use_format is None:
            use_format = guess_format(cached_version_path[:-3])
            use_format = use_format if use_format is not None else 'xml'

        with gzip.open(cached_version_path, 'r') as vocab_data:
            (memory_graph if use_batch else graph).parse(vocab_data, format=use_format)

        if use_batch:
            with BatchAddGraph(graph, batch_size=200) as batch:
                for triple in memory_graph:
                    batch.add(triple)
    except xml.sax._exceptions.SAXParseException:
        if format is None:
            load_cached_into_graph(graph, cached_version_path, use_batch, 'ttl')
        else:
            raise Exception(f"Failed to parse RDF data in {cached_version_path} with format {format}")


def load_cached_into_remote(graph_uri: str, cached_version_path: str, format: str = None) -> None:
    try:
        if format is None:
            format = guess_format(cached_version_path[:-3])
            format = format if format is not None else 'xml'

        content_types = {
            'xml': 'application/rdf+xml',
            'turtle': 'text/turtle',
            'ttl': 'text/turtle',
            'nt': 'application/n-triples',
            'n3': 'text/n3',
            'json-ld': 'application/ld+json',
            'trig': 'application/trig',
            'nquads': 'application/n-quads',
        }
        content_type = content_types.get(format, 'application/rdf+xml')

        with open(cached_version_path, 'rb') as vocab_data:
            params = {'graph': graph_uri}
            headers = {'Content-Type': content_type}
            auth = (sparql_user, sparql_password) if sparql_user else None

            response = requests.post(
                sparql_update_url,
                params=params,
                data=vocab_data,
                headers=headers,
                auth=auth
            )
            response.raise_for_status()
    except requests.RequestException as e:
        if format is None:
            load_cached_into_remote(graph_uri, cached_version_path, 'ttl')
        else:
            raise Exception(f"Failed to load data into SPARQL store: {e}")


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
