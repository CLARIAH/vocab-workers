from pydantic import BaseModel
from rdflib import Graph, RDF, XSD, URIRef, Literal
from rdflib.term import Node

from vocab.util.rdf import load_cached_into_graph


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
    prefixes: dict[str, str] = {}
    stats: dict[str, int] = {}
    subjects: SummaryPart = SummaryPart()
    predicates: SummaryPart = SummaryPart()
    objects: ObjectsSummaryPart = ObjectsSummaryPart()


def summarize(path: str) -> Summary:
    def count_for(instance: Node, summary_part: SummaryPart) -> None:
        if isinstance(instance, URIRef):
            summary_part.count += 1

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

    graph = Graph(bind_namespaces='none')
    load_cached_into_graph(graph, path)

    summary = Summary(prefixes={prefix: str(namespace)
                                for prefix, namespace in graph.namespaces()})

    for s, p, o in graph:
        count_for(s, summary.subjects)
        count_for(p, summary.predicates)
        count_for(o, summary.objects)

        if isinstance(o, URIRef) and p == RDF.type:
            classes_count_for(o, summary.objects.classes)

        if isinstance(o, Literal):
            datatype = o.datatype if o.datatype else XSD['langString'] if o.language else XSD.string
            classes_count_for(datatype, summary.objects.literals)

            if o.language:
                summary.objects.literals.languages[o.language] = \
                    summary.objects.literals.languages.get(o.language, 0) + 1

    return summary


summary = summarize('/Users/kerim/git/vocab-workers/data/cache/as/2.0.ttl.gz')
print(summary.model_dump_json())
