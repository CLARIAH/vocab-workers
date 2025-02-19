import sys
import logging

from lxml import etree
from lxml.etree import Element
from pydantic import BaseModel

from rdflib.term import Node
from rdflib import Graph, RDF, XSD, URIRef, Literal

from vocab.app import celery
from vocab.cmdi import with_version_and_dump, cmdi_from_redis
from vocab.util.file import run_work_for_file
from vocab.util.rdf import load_cached_into_graph
from vocab.util.xml import ns, ns_prefix, voc_root, grab_first

log = logging.getLogger(__name__)


class ClassesSummary(BaseModel):
    count: int = 0
    stats: dict[str, dict[str, int]] = {}


class LiteralsSummary(ClassesSummary):
    languages: dict[str, int] = {}


class SummaryPart(BaseModel):
    count: int = 0
    stats: dict[str, int] = {}


class ObjectsSummaryPart(SummaryPart):
    classes: ClassesSummary = ClassesSummary()
    literals: LiteralsSummary = LiteralsSummary()


class Summary(BaseModel):
    total: int = 0
    prefixes: dict[str, str] = {}
    stats: dict[str, int] = {}
    subjects: SummaryPart = SummaryPart()
    predicates: SummaryPart = SummaryPart()
    objects: ObjectsSummaryPart = ObjectsSummaryPart()


@celery.task(name='rdf.summarizer', autoretry_for=(Exception,),
             default_retry_delay=60 * 30, retry_kwargs={'max_retries': 5})
def summarizer(nr: int, id: int) -> None:
    for record, version, cached_version_path in with_version_and_dump(nr, id):
        if record.type.syntax in ['owl', 'skos', 'rdfs']:
            try:
                summary = summarize(cached_version_path)
                write_summary_statements(nr, id, version.version, summary)
            except Exception as e:
                log.error(f'Failed to summarize for {record.identifier} and version {version.version}: {e}')


def summarize(path: str) -> Summary:
    def count_for(instance: Node, summary_part: SummaryPart, distinct: set) -> None:
        distinct.add(instance)

        if isinstance(instance, URIRef):
            try:
                prefix, namespace, name = graph.compute_qname(instance)
                summary.stats[prefix] = summary.stats.get(prefix, 0) + 1
                summary_part.stats[prefix] = summary_part.stats.get(prefix, 0) + 1
            except:
                pass

    def classes_count_for(instance: URIRef, classes_summary: ClassesSummary) -> None:
        classes_summary.count += 1

        try:
            prefix, namespace, name = graph.compute_qname(instance)
            prefix_stats = classes_summary.stats.get(prefix, {})
            prefix_stats[name] = prefix_stats.get(name, 0) + 1
            classes_summary.stats[prefix] = prefix_stats
        except:
            pass

    graph = Graph(bind_namespaces='core')
    load_cached_into_graph(graph, path)

    summary = Summary(prefixes={prefix: str(namespace)
                                for prefix, namespace in graph.namespaces()})

    distinct = {"subjects": set(), "predicates": set(), "objects": set()}

    for s, p, o in graph:
        summary.total += 1

        count_for(s, summary.subjects, distinct["subjects"])
        count_for(p, summary.predicates, distinct["predicates"])
        count_for(o, summary.objects, distinct["objects"])

        if isinstance(o, URIRef) and p == RDF.type:
            classes_count_for(o, summary.objects.classes)

        if isinstance(o, Literal):
            datatype = o.datatype if o.datatype else RDF.langString if o.language else XSD.string
            classes_count_for(datatype, summary.objects.literals)

            if o.language:
                summary.objects.literals.languages[o.language] = \
                    summary.objects.literals.languages.get(o.language, 0) + 1

    summary.subjects.count = len(distinct["subjects"])
    summary.predicates.count = len(distinct["predicates"])
    summary.objects.count = len(distinct["objects"])

    return summary


def write_summary_statements(nr: int, id: int, version: str, summary: Summary) -> None:
    def write_namespaces(root: Element, stats: dict[str, int], prefixes: dict[str, str]) -> None:
        namespaces = etree.SubElement(root, f"{ns_prefix}Namespaces", nsmap=ns)
        for prefix, count in stats.items():
            if prefix in prefixes:
                namespace = etree.SubElement(namespaces, f"{ns_prefix}Namespace", nsmap=ns)

                uri_elem = etree.SubElement(namespace, f"{ns_prefix}URI", nsmap=ns)
                uri_elem.text = summary.prefixes[prefix]

                prefix_elem = etree.SubElement(namespace, f"{ns_prefix}prefix", nsmap=ns)
                prefix_elem.text = prefix

                count_elem = etree.SubElement(namespace, f"{ns_prefix}count", nsmap=ns)
                count_elem.text = str(count)

    def write_namespace_items(root: Element, stats: dict[str, dict[str, int]], prefixes: dict[str, str]) -> None:
        namespace_items_elem = etree.SubElement(root, f"{ns_prefix}NamespaceItems", nsmap=ns)
        for prefix, name_counts in stats.items():
            if prefix in prefixes:
                for name, count in name_counts.items():
                    namespace_item_elem = etree.SubElement(namespace_items_elem, f"{ns_prefix}NamespaceItem", nsmap=ns)

                    namespace_item_uri_elem = etree.SubElement(namespace_item_elem, f"{ns_prefix}URI", nsmap=ns)
                    namespace_item_uri_elem.text = summary.prefixes[prefix]

                    namespace_item_prefix_elem = etree.SubElement(namespace_item_elem, f"{ns_prefix}prefix", nsmap=ns)
                    namespace_item_prefix_elem.text = prefix

                    namespace_item_name_elem = etree.SubElement(namespace_item_elem, f"{ns_prefix}name", nsmap=ns)
                    namespace_item_name_elem.text = name

                    namespace_item_count_elem = etree.SubElement(namespace_item_elem, f"{ns_prefix}count", nsmap=ns)
                    namespace_item_count_elem.text = str(count)

    with cmdi_from_redis(nr, id) as root:
        version_elem = grab_first(f"{voc_root}/cmd:Version/cmd:version[text()='{version}']/..", root)
        if version_elem is not None:
            summary_elem = grab_first("./cmd:Summary", version_elem)
            # if summary_elem is not None:
            #     version_elem.remove(summary_elem)
            if summary_elem is None:
                summary_elem = etree.SubElement(version_elem, f"{ns_prefix}Summary", nsmap=ns)

            namespaces = grab_first("./cmd:Namespaces", summary_elem)
            if namespaces is not None:
                summary_elem.remove(namespaces)

            statements = grab_first("./cmd:Statements", summary_elem)
            if statements is not None:
                summary_elem.remove(statements)

            statements = etree.SubElement(summary_elem, f"{ns_prefix}Statements", nsmap=ns)

            subjects = etree.SubElement(statements, f"{ns_prefix}Subjects", nsmap=ns)
            subjects_count = etree.SubElement(subjects, f"{ns_prefix}count", nsmap=ns)
            subjects_count.text = str(summary.subjects.count)

            predicates = etree.SubElement(statements, f"{ns_prefix}Predicates", nsmap=ns)
            predicates_count = etree.SubElement(predicates, f"{ns_prefix}count", nsmap=ns)
            predicates_count.text = str(summary.predicates.count)

            objects = etree.SubElement(statements, f"{ns_prefix}Objects", nsmap=ns)
            objects_count = etree.SubElement(objects, f"{ns_prefix}count", nsmap=ns)
            objects_count.text = str(summary.objects.count)

            object_classes = etree.SubElement(objects, f"{ns_prefix}Classes", nsmap=ns)
            object_classes_count = etree.SubElement(object_classes, f"{ns_prefix}count", nsmap=ns)
            object_classes_count.text = str(summary.objects.classes.count)

            object_literals = etree.SubElement(objects, f"{ns_prefix}Literals", nsmap=ns)
            object_literals_count = etree.SubElement(object_literals, f"{ns_prefix}count", nsmap=ns)
            object_literals_count.text = str(summary.objects.literals.count)

            literal_languages = etree.SubElement(object_literals, f"{ns_prefix}Languages", nsmap=ns)
            if summary.objects.literals.languages:
                for lang, count in summary.objects.literals.languages.items():
                    literal_language = etree.SubElement(literal_languages, f"{ns_prefix}Language", nsmap=ns)

                    literal_language_code = etree.SubElement(literal_language, f"{ns_prefix}code", nsmap=ns)
                    literal_language_code.text = lang

                    literal_language_count = etree.SubElement(literal_language, f"{ns_prefix}count", nsmap=ns)
                    literal_language_count.text = str(count)

            write_namespaces(summary_elem, summary.stats, summary.prefixes)
            write_namespaces(subjects, summary.subjects.stats, summary.prefixes)
            write_namespaces(predicates, summary.predicates.stats, summary.prefixes)
            write_namespaces(objects, summary.objects.stats, summary.prefixes)
            write_namespaces(object_classes, {prefix: sum(name_counts.values())
                                              for prefix, name_counts in summary.objects.classes.stats.items()},
                             summary.prefixes)
            write_namespaces(object_literals, {prefix: sum(name_counts.values())
                                               for prefix, name_counts in summary.objects.literals.stats.items()},
                             summary.prefixes)

            write_namespace_items(object_classes, summary.objects.classes.stats, summary.prefixes)
            write_namespace_items(object_literals, summary.objects.literals.stats, summary.prefixes)


if __name__ == '__main__':
    with run_work_for_file(sys.argv[1]) as (nr, id):
        summarizer(nr, id)
