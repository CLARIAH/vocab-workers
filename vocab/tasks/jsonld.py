import os
import sys
import json
import gzip

from pyld import jsonld
from bs4 import BeautifulSoup
from markdown import markdown
from urllib.parse import quote
from importlib.resources import files
from rdflib import Namespace, Graph, DCAT, DCTERMS, SDO, VOID, FOAF, RDF, Literal, URIRef, XSD, BNode, PROV

from vocab.app import celery
from vocab.cmdi import get_record, Vocab, Version, Review, Authority
from vocab.config import root_path, jsonld_rel_path, vocab_registry_url
from vocab.util.rdf import get_sparql_store
from vocab.util.work import get_files_in_path, run_work_for_file

VOCAB = Namespace(vocab_registry_url + '#')
MOD = Namespace('https://w3id.org/mod#')
XTYPES = Namespace('http://purl.org/xtypes/')
VANN = Namespace('http://purl.org/vocab/vann/')

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


@celery.task(name='jsonld', autoretry_for=(Exception,),
             default_retry_delay=60 * 30, retry_kwargs={'max_retries': 5})
def create_jsonld(nr: int, id: int) -> None:
    record = get_record(nr, id)
    # TODO: current_graph = get_current_jsonld(record.identifier)

    new_graph = init_graph()
    create_rdf_in_graph(record, new_graph)

    # TODO: replace_in_sparql_store(current_graph, new_graph)

    jsonld_output = json.loads(new_graph.serialize(format='json-ld', context=CONTEXT))
    jsonld_framed = jsonld.frame(jsonld_output, FRAME)
    del jsonld_framed['@context']

    jsonld_data = json.dumps(jsonld_framed, indent=4)
    jsonld_data = bytes(jsonld_data, 'utf-8')
    # TODO: jsonld_data = gzip.compress(jsonld_data)
    open(os.path.join(root_path, jsonld_rel_path, record.identifier + '.jsonld'), 'wb').write(jsonld_data)  # TODO: .gz

    # TODO: Also write TTL for now:
    ttl_data = new_graph.serialize(format='ttl')
    open(os.path.join(root_path, jsonld_rel_path, record.identifier + '.ttl'), 'w').write(ttl_data)


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
    graph.bind('mod', MOD)
    graph.bind('dcterms', DCTERMS)
    graph.bind('xtypes', XTYPES)
    graph.bind('prov', PROV)
    graph.bind('schema', SDO)
    graph.bind('void', VOID)
    graph.bind('foaf', FOAF)
    graph.bind('vann', VANN)

    return graph


def create_rdf_in_graph(cmdi: Vocab, graph: Graph) -> None:
    uri = URIRef(VOCAB[cmdi.identifier])
    create_catalog_record_in_graph(cmdi, uri, graph)

    graph.add((uri, RDF.type, DCAT.Dataset))
    graph.add((uri, RDF.type, MOD.SemanticArtifact))
    graph.add((uri, DCTERMS.identifier, Literal(cmdi.identifier)))
    graph.add((uri, DCTERMS.conformsTo, URIRef(CONFORMS_TO[cmdi.type.syntax])))
    graph.add((uri, DCTERMS.title, Literal(cmdi.title, lang='en')))

    if cmdi.description is not None:
        description_html = markdown(cmdi.description)
        description_soup = BeautifulSoup(description_html, 'html.parser')
        description_text = ''.join(description_soup.find_all(string=True)).strip()

        graph.add((uri, DCTERMS.description, Literal(description_text, lang='en')))
        graph.add((uri, DCTERMS.description, Literal(cmdi.description, datatype=XTYPES['Fragment-Markdown'])))

    for loc in cmdi.locations:
        if loc.type == 'homepage' and loc.recipe is None:
            graph.add((uri, DCAT.landingPage, URIRef(loc.location)))

    for license in cmdi.licenses:
        if license.uri:
            graph.add((uri, DCTERMS.license, URIRef(license.uri)))

    for language in cmdi.languages:
        graph.add((uri, DCTERMS.language, Literal(language)))

    # if cmdi.topic.unesco:
    #     graph.add((uri, DCAT.theme, URIRef(cmdi.topic.unesco)))
    # if cmdi.topic.nwo:
    #     graph.add((uri, DCAT.theme, URIRef(cmdi.topic.nwo)))
    # if cmdi.type.kos:
    #     graph.add((uri, DCAT.theme, URIRef(cmdi.type.kos)))
    # if cmdi.type.entity:
    #     graph.add((uri, DCTERMS.type, URIRef(cmdi.type.entity)))

    for keyword in cmdi.keywords:
        if keyword.uri:
            keyword_node = BNode()
            graph.add((uri, DCTERMS.subject, keyword_node))
            graph.add((keyword_node, RDF.type, SDO.DefinedTerm))
            graph.add((keyword_node, SDO.name, Literal(keyword.label, 'en')))
            graph.add((keyword_node, SDO.sameAs, URIRef(keyword.uri)))
        else:
            graph.add((uri, DCAT.keyword, Literal(keyword.label, 'en')))

    for creator in cmdi.creators:
        create_authorities_in_graph(cmdi, uri, creator, URIRef('urn:example:isotc211/CI_RoleCode/originator'), graph)
    for maintainer in cmdi.maintainers:
        create_authorities_in_graph(cmdi, uri, maintainer, URIRef('urn:example:isotc211/CI_RoleCode/custodian'), graph)
    for contributor in cmdi.contributors:
        create_authorities_in_graph(cmdi, uri, contributor, URIRef('urn:example:isotc211/CI_RoleCode/contributor'),
                                    graph)

    if cmdi.namespace and cmdi.namespace.prefix:
        graph.add((uri, VANN.preferredNamespaceUri, URIRef(cmdi.namespace.uri)))
        graph.add((uri, VANN.preferredNamespacePrefix, Literal(cmdi.namespace.prefix)))

    for registry in cmdi.registries:
        registery_url_name = quote(registry.title)

        vocab_in_registry_url = URIRef(VOCAB[cmdi.identifier + '_registry_' + registery_url_name])
        graph.add((uri, DCTERMS.isReferencedBy, vocab_in_registry_url))
        graph.add((vocab_in_registry_url, RDF.type, DCAT.Dataset))
        graph.add((vocab_in_registry_url, RDF.type, MOD.SemanticArtifact))
        graph.add((vocab_in_registry_url, DCTERMS.title, Literal(cmdi.title, lang='en')))
        graph.add((vocab_in_registry_url, DCAT.landingPage,
                   URIRef(registry.landing_page if registry.landing_page else registry.url)))

        registry_catalog_uri = URIRef(VOCAB['registry_' + registery_url_name])
        graph.add((vocab_in_registry_url, DCAT.inCatalog, registry_catalog_uri))
        graph.add((registry_catalog_uri, RDF.type, DCAT.Catalog))
        graph.add((registry_catalog_uri, RDF.type, MOD.SemanticArtefactCatalog))
        graph.add((registry_catalog_uri, DCTERMS.title, Literal(registry.title, lang='en')))
        graph.add((registry_catalog_uri, FOAF.homepage, URIRef(registry.url)))
        graph.add((registry_catalog_uri, DCAT.record, vocab_in_registry_url))

    for review in cmdi.reviews:
        create_review_rdf_in_graph(cmdi, uri, review, graph)

    for version in cmdi.versions:
        create_version_rdf_in_graph(cmdi, uri, version, graph)


def create_catalog_record_in_graph(cmdi: Vocab, uri: URIRef, graph: Graph):
    catalog_record_uri = URIRef(VOCAB[cmdi.identifier + '_record'])
    graph.add((uri, FOAF.isPrimaryTopicOf, catalog_record_uri))
    graph.add((catalog_record_uri, RDF.type, DCAT.CatalogRecord))
    graph.add((catalog_record_uri, RDF.type, MOD.SemanticArtefactCatalogRecord))
    graph.add((catalog_record_uri, FOAF.primaryTopic, uri))
    graph.add((catalog_record_uri, DCTERMS.conformsTo, URIRef('https://www.w3.org/TR/vocab-dcat/')))
    graph.add((catalog_record_uri, DCTERMS.issued, Literal(cmdi.created, datatype=XSD.date)))
    graph.add((catalog_record_uri, DCTERMS.modified, Literal(cmdi.modified, datatype=XSD.date)))

    catalog_uri = URIRef(VOCAB)
    graph.add((catalog_record_uri, DCAT.inCatalog, catalog_uri))
    graph.add((catalog_uri, RDF.type, DCAT.Catalog))
    graph.add((catalog_uri, RDF.type, MOD.SemanticArtefactCatalog))
    graph.add((catalog_uri, DCTERMS.title, Literal('Vocabulary registry', lang='en')))
    graph.add((catalog_uri, FOAF.homepage, URIRef(VOCAB)))
    graph.add((catalog_uri, DCAT.record, catalog_record_uri))


def create_authorities_in_graph(cmdi: Vocab, uri: URIRef, authority: Authority, role: URIRef, graph: Graph):
    authority_node = BNode()
    graph.add((uri, PROV.qualifiedAttribution, authority_node))
    graph.add((authority_node, RDF.type, PROV.Attribution))
    graph.add((authority_node, DCAT.hadRole, role))

    agent_node = BNode()
    graph.add((authority_node, PROV.agent, agent_node))
    graph.add((agent_node, RDF.type, FOAF.Person))
    graph.add((agent_node, FOAF.name, Literal(authority.label)))
    if authority.uri:
        graph.add((agent_node, FOAF.homepage, URIRef(authority.uri)))


def create_review_rdf_in_graph(cmdi: Vocab, uri: URIRef, review: Review, graph: Graph) -> None:
    review_uri = URIRef(VOCAB[f'{cmdi.identifier}_review_{review.id}'])
    graph.add((uri, SDO.review, review_uri))
    graph.add((review_uri, RDF.type, SDO.Review))
    graph.add((review_uri, SDO.itemReviewed, uri))
    graph.add((review_uri, SDO.reviewBody, Literal(review.body)))

    review_rating = BNode()
    graph.add((review_uri, SDO.reviewRating, review_rating))
    graph.add((review_rating, RDF.type, SDO.Rating))
    graph.add((review_rating, SDO.worstRating, Literal(0.5)))
    graph.add((review_rating, SDO.bestRating, Literal(1)))
    graph.add((review_rating, SDO.ratingValue, Literal(review.rating)))

    like_action = BNode()
    graph.add((review_uri, SDO.interactionStatistic, like_action))
    graph.add((like_action, RDF.type, SDO.InteractionCounter))
    graph.add((like_action, SDO.interactionType, SDO.LikeAction))
    graph.add((like_action, SDO.userInteractionCount, Literal(review.likes)))

    dislike_action = BNode()
    graph.add((review_uri, SDO.interactionStatistic, dislike_action))
    graph.add((dislike_action, RDF.type, SDO.InteractionCounter))
    graph.add((dislike_action, SDO.interactionType, SDO.DislikeAction))
    graph.add((dislike_action, SDO.userInteractionCount, Literal(review.dislikes)))


def create_version_rdf_in_graph(cmdi: Vocab, uri: URIRef, version: Version, graph: Graph) -> None:
    version_uri = URIRef(VOCAB[f'{cmdi.identifier}_{version.version}'])
    graph.add((uri, DCTERMS.hasVersion, version_uri))

    graph.add((version_uri, RDF.type, DCAT.Dataset))
    graph.add((version_uri, RDF.type, MOD.SemanticArtifact))
    graph.add((version_uri, DCTERMS.title, Literal(f'{cmdi.title} {version.version}')))
    graph.add((version_uri, DCAT.version, Literal(version.version)))
    graph.add((version_uri, DCAT.isVersionOf, uri))
    graph.add((version_uri, DCTERMS.issued, Literal(version.validFrom, datatype=XSD.date)))

    distribution_url = None
    cache_url = None
    endpoint_url = None
    for loc in version.locations:
        if loc.type == 'homepage':
            graph.add((version_uri, DCAT.landingPage, URIRef(loc.location)))
        elif loc.type == 'dump':
            if loc.recipe == 'cache':
                cache_url = loc.location
            else:
                distribution_url = loc.location
        elif loc.type == 'endpoint':
            endpoint_url = loc.location

    if distribution_url:
        distribution_uri = URIRef(distribution_url)
        graph.add((version_uri, DCAT.distribution, distribution_uri))
        graph.add((distribution_uri, RDF.type, DCAT.Distribution))
        graph.add((distribution_uri, DCTERMS.title, Literal(f'Distribution of {cmdi.title} {version.version}')))
        graph.add((distribution_uri, DCAT.downloadURL, URIRef(distribution_url)))

        if cache_url:
            cache_uri = URIRef(cache_url)
            graph.add((cache_uri, RDF.type, DCAT.Distribution))
            graph.add((cache_uri, DCTERMS.title, Literal(f'Cached distribution of {cmdi.title} {version.version}')))
            graph.add((cache_uri, DCAT.downloadURL, URIRef(cache_url)))
            graph.add((cache_uri, PROV.wasDerivedFrom, distribution_uri))

        if endpoint_url:
            data_service_uri = URIRef(endpoint_url)
            graph.add((distribution_uri, DCAT.accessService, data_service_uri))
            graph.add((data_service_uri, RDF.type, DCAT.DataService))
            graph.add((data_service_uri, DCTERMS.title, Literal(f'SPARQL endpoint of {cmdi.title} {version.version}')))
            graph.add((data_service_uri, DCAT.accessURL, URIRef(endpoint_url)))
            graph.add((data_service_uri, DCTERMS.conformsTo, URIRef(RECIPE['sparql'])))

    # if version.summary is not None:
    #     create_version_summary_rdf_in_graph(cmdi, version_uri, version, graph)


def create_version_summary_rdf_in_graph(cmdi: Vocab, version_uri: URIRef, version: Version, graph: Graph) -> None:
    summary = version.summary
    summary_uri = URIRef(VOCAB[f'{cmdi.identifier}/version/{version.version}/summary'])
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


if __name__ == '__main__':
    for f in get_files_in_path(sys.argv[1]):
        with run_work_for_file(f) as (nr, id):
            create_jsonld(nr, id)
