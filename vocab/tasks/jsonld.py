import os
import json
import gzip

from pyld import jsonld
from bs4 import BeautifulSoup
from markdown import markdown
from importlib.resources import files
from rdflib import Namespace, Graph, DCAT, DCTERMS, SDO, VOID, RDF, Literal, URIRef, XSD, BNode

from vocab.app import celery
from vocab.cmdi import get_record, Vocab, Version, Review
from vocab.config import root_path, jsonld_rel_path, vocab_namespace
from vocab.util.rdf import get_sparql_store

VOCAB = Namespace(vocab_namespace)
XTYPES = Namespace('http://purl.org/xtypes/')

CONTEXT = json.loads(files('vocab.util').joinpath('context.json').read_bytes())

FRAME = {
    "@context": CONTEXT,
    "versions": {
        "summary": {}
    }
}

CONFORMS_TO = {
    "skos": "https://www.w3.org/TR/skos-reference/",
    "owl": "https://www.w3.org/TR/owl2-overview/",
    "xsd": "https://www.w3.org/TR/xmlschema-0/",
    "relaxng": "https://www.iso.org/standard/52348.html",
    "shacl": "https://www.w3.org/TR/shacl/",
    "sql ddl": "https://www.iso.org/standard/76583.html",
    "cow": "https://www.w3.org/TR/tabular-data-primer/",
    "linkml": "https://linkml.io/",
    "rdfs": "https://www.w3.org/TR/rdf-schema/"
}

RECIPE = {
    "sparql": "https://www.w3.org/TR/sparql11-query/",
    "skosmos": "https://skosmos.org",
    "elastic": "https://www.elastic.co/elasticsearch",
    "solr": "https://solr.apache.org",
    "rdf": "https://www.w3.org/RDF/"
}

PUBLISHER = {
    "yalc": "https://github.com/TriplyDB/YALC",
    "awesome humanities": "https://github.com/CLARIAH/awesome-humanities-ontologies"
}


@celery.task(name='jsonld')
def create_jsonld(id: str) -> None:
    record = get_record(id)
    current_graph = get_current_jsonld(id)

    new_graph = init_graph()
    create_rdf_in_graph(record, new_graph)

    replace_in_sparql_store(current_graph, new_graph)

    jsonld_output = json.loads(new_graph.serialize(format='json-ld', context=CONTEXT))
    jsonld_framed = jsonld.frame(jsonld_output, FRAME)
    del jsonld_framed['@context']

    jsonld_data = json.dumps(jsonld_framed, indent=4)
    jsonld_data = bytes(jsonld_data, 'utf-8')
    jsonld_data = gzip.compress(jsonld_data)
    open(os.path.join(root_path, jsonld_rel_path, id + '.jsonld.gz'), 'wb').write(jsonld_data)


def get_current_jsonld(id: str) -> Graph | None:
    jsonld_file = os.path.join(root_path, jsonld_rel_path, id + '.json.gz')
    if os.path.exists(jsonld_file):
        graph = Graph(bind_namespaces='core')
        with gzip.open(jsonld_file, 'r') as jsonld_data:
            graph.parse(jsonld_data.read(), format='json-ld', context=CONTEXT)

        return graph

    return None


def init_graph() -> Graph:
    graph = Graph(bind_namespaces='core')
    graph.bind('vocab', VOCAB)
    graph.bind('dcat', DCAT)
    graph.bind('dcterms', DCTERMS)
    graph.bind('xtypes', XTYPES)
    graph.bind('schema', SDO)
    graph.bind('void', VOID)

    return graph


def create_rdf_in_graph(cmdi: Vocab, graph: Graph) -> None:
    uri = URIRef(VOCAB[cmdi.id])

    graph.add((uri, RDF.type, DCAT.Dataset))
    graph.add((uri, DCTERMS.identifier, Literal(cmdi.id)))
    graph.add((uri, DCTERMS.title, Literal(cmdi.title, lang='en')))
    graph.add((uri, DCTERMS.conformsTo, URIRef(CONFORMS_TO[cmdi.type])))

    for loc in cmdi.locations:
        if loc.type == 'homepage' and loc.recipe is None:
            graph.add((uri, DCAT.landingPage, URIRef(loc.location)))

    if cmdi.description is not None:
        description_html = markdown(cmdi.description)
        description_soup = BeautifulSoup(description_html, 'html.parser')
        description_text = ''.join(description_soup.findAll(string=True)).strip()

        graph.add((uri, DCTERMS.description, Literal(description_text, lang='en')))
        graph.add((uri, DCTERMS.description, Literal(cmdi.description, datatype=XTYPES['Fragment-Markdown'])))

    graph.add((uri, DCTERMS.license, URIRef(cmdi.license.uri)))

    # graph.add((uri, DCTERMS.issued, Literal(cmdi.created, datatype=XSD.date)))
    # graph.add((uri, DCTERMS.modified, Literal(cmdi.modified, datatype=XSD.date)))

    for publisher in cmdi.publishers:
        graph.add((uri, DCTERMS.publisher, URIRef(PUBLISHER[publisher.uri.lower()])))

    for review in cmdi.reviews:
        create_review_rdf_in_graph(cmdi, uri, review, graph)

    for version in cmdi.versions:
        create_version_rdf_in_graph(cmdi, uri, version, graph)


def create_review_rdf_in_graph(cmdi: Vocab, uri: URIRef, review: Review, graph: Graph) -> None:
    review_uri = URIRef(VOCAB[f'{cmdi.id}/review/{review.id}'])
    graph.add((uri, SDO.review, review_uri))

    review_rating = BNode()
    like_action = BNode()
    dislike_action = BNode()

    graph.add((review_uri, RDF.type, SDO.Review))
    graph.add((review_uri, SDO.itemReviewed, uri))
    graph.add((review_uri, SDO.reviewRating, review_rating))
    graph.add((review_uri, SDO.reviewBody, Literal(review.review)))
    graph.add((review_uri, SDO.interactionStatistic, like_action))
    graph.add((review_uri, SDO.interactionStatistic, dislike_action))

    graph.add((review_rating, RDF.type, SDO.Rating))
    graph.add((review_rating, SDO.worstRating, Literal(0.5)))
    graph.add((review_rating, SDO.bestRating, Literal(1)))
    graph.add((review_rating, SDO.ratingValue, Literal(review.rating)))

    graph.add((like_action, RDF.type, SDO.InteractionCounter))
    graph.add((like_action, SDO.interactionType, SDO.LikeAction))
    graph.add((like_action, SDO.userInteractionCount, Literal(review.likes)))

    graph.add((dislike_action, RDF.type, SDO.InteractionCounter))
    graph.add((dislike_action, SDO.interactionType, SDO.DislikeAction))
    graph.add((dislike_action, SDO.userInteractionCount, Literal(review.dislikes)))


def create_version_rdf_in_graph(cmdi: Vocab, uri: URIRef, version: Version, graph: Graph) -> None:
    version_uri = URIRef(VOCAB[f'{cmdi.id}/version/{version.version}'])
    graph.add((uri, DCTERMS.hasVersion, version_uri))

    graph.add((version_uri, RDF.type, DCAT.Dataset))
    graph.add((version_uri, DCTERMS.title, Literal(f'{cmdi.title} {version.version}')))
    graph.add((version_uri, DCAT.version, Literal(version.version)))
    graph.add((version_uri, DCAT.isVersionOf, URIRef(uri)))
    graph.add((version_uri, DCTERMS.issued, Literal(version.validFrom, datatype=XSD.date)))

    for loc in version.locations:
        if loc.type == 'homepage':
            graph.add((version_uri, DCAT.landingPage, URIRef(loc.location)))
        elif loc.type == 'dump':
            distribution = BNode()
            graph.add((version_uri, DCAT.distribution, distribution))
            graph.add((distribution, RDF.type, DCAT.Distribution))
            graph.add((distribution, DCAT.downloadURL, URIRef(loc.location)))
        elif loc.type == 'endpoint':
            data_service = BNode()
            graph.add((version_uri, DCAT.accessService, data_service))
            graph.add((data_service, RDF.type, DCAT.DataService))
            graph.add((data_service, DCAT.accessURL, URIRef(loc.location)))
            graph.add((data_service, DCTERMS.conformsTo, URIRef(RECIPE[loc.recipe])))

    if version.summary is not None:
        create_version_summary_rdf_in_graph(cmdi, version_uri, version, graph)


def create_version_summary_rdf_in_graph(cmdi: Vocab, version_uri: URIRef, version: Version, graph: Graph) -> None:
    summary = version.summary
    summary_uri = URIRef(VOCAB[f'{cmdi.id}/version/{version.version}/summary'])
    graph.add((summary_uri, RDF.type, VOID.Dataset))
    graph.add((summary_uri, DCTERMS.isPartOf, version_uri))

    for stat in summary.stats.stats:
        subj_count = next((subj_stat.count for subj_stat in summary.subjects.stats
                           if subj_stat.prefix == stat.prefix), 0)
        pred_count = next((pred_stat.count for pred_stat in summary.predicates.stats
                           if pred_stat.prefix == stat.prefix), 0)
        obj_count = next((obj_stat.count for obj_stat in summary.objects.stats
                          if obj_stat.prefix == stat.prefix), 0)

        graph.add((summary_uri, VOID.vocabulary, URIRef(stat.uri)))

        prefix_node = BNode()
        graph.add((summary_uri, VOCAB['vocabulary'], prefix_node))
        graph.add((prefix_node, VOCAB['prefix'], Literal(stat.prefix)))
        graph.add((prefix_node, VOCAB['uri'], URIRef(stat.uri)))

        graph.add((prefix_node, VOCAB['distinctOccurrences'], Literal(stat.count)))
        graph.add((prefix_node, VOID.distinctSubjects, Literal(subj_count)))
        graph.add((prefix_node, VOID.properties, Literal(pred_count)))
        graph.add((prefix_node, VOID.distinctObjects, Literal(obj_count)))

        for class_stat in summary.objects.classes.list:
            if class_stat.prefix == stat.prefix:
                class_partition = BNode()
                graph.add((prefix_node, VOID.classPartition, class_partition))
                graph.add((class_partition, VOID['class'], URIRef(class_stat.uri + class_stat.name)))
                graph.add((class_partition, VOCAB['name'], Literal(class_stat.name)))
                graph.add((class_partition, VOID.entities, Literal(class_stat.count)))

        for literal_stat in summary.objects.literals.list:
            if literal_stat.prefix == stat.prefix:
                literal_partition = BNode()
                graph.add((prefix_node, VOCAB['literalPartition'], literal_partition))
                graph.add((literal_partition, VOID['class'], URIRef(literal_stat.uri + literal_stat.name)))
                graph.add((literal_partition, VOCAB['name'], Literal(literal_stat.name)))
                graph.add((literal_partition, VOID.entities, Literal(literal_stat.count)))

    # graph.add((summary_uri, VOID.triples, Literal(summary.stats.count)))
    graph.add((summary_uri, VOID.distinctSubjects, Literal(summary.subjects.count)))
    graph.add((summary_uri, VOID.properties, Literal(summary.predicates.count)))
    graph.add((summary_uri, VOID.distinctObjects, Literal(summary.objects.count)))
    graph.add((summary_uri, VOID.entities, Literal(summary.objects.classes.count)))
    graph.add((summary_uri, VOCAB['literals'], Literal(summary.objects.literals.count)))

    for (lang, count) in summary.objects.literals.languages.items():
        languages = BNode()
        graph.add((summary_uri, VOCAB['languages'], languages))
        graph.add((languages, VOCAB['language'], Literal(lang)))
        graph.add((languages, VOID.triples, Literal(count)))


def replace_in_sparql_store(old_graph: Graph | None, new_graph: Graph):
    sparql_store = get_sparql_store(False)
    graph = Graph(store=sparql_store)
    nts = sparql_store.node_to_sparql

    if old_graph:
        for (s, p, o) in old_graph:
            graph.update("DELETE { %s %s %s . } WHERE { %s %s %s . }" %
                         (nts(s), nts(p), nts(o), nts(s), nts(p), nts(o)))

    sparql_add = ["%s %s %s ." % (nts(s), nts(p), nts(o)) for (s, p, o) in new_graph]
    graph.update("INSERT DATA { %s }" % '\n'.join(sparql_add))
