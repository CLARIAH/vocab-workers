import os
import logging
import elementpath
import unicodedata

from lxml import etree
from lxml.etree import Element
from datetime import datetime
from pydantic import BaseModel
from inspect import cleandoc
from typing import Any, Optional, List, Generator, Tuple

from vocab.config import records_path
from vocab.util.fs import get_cached_version

log = logging.getLogger(__name__)

ns = {"cmd": "http://www.clarin.eu/cmd/1"}
ns_prefix = '{http://www.clarin.eu/cmd/1}'
voc_root = './cmd:Components/cmd:Vocabulary'

xpath_code = "./cmd:code"
xpath_count = "./cmd:count"
xpath_name = "./cmd:name"
xpath_type = "./cmd:type"
xpath_recipe = "./cmd:recipe"
xpath_URI = "./cmd:URI"
xpath_uri = "./cmd:uri"
xpath_prefix = "./cmd:prefix"
xpath_version_no = "./cmd:version"
xpath_valid_from = "./cmd:validFrom"

xpath_location_elem = "./cmd:Location"
xpath_namespace_elem = "./cmd:Namespaces/cmd:Namespace"
xpath_list_item_elem = "./cmd:List/cmd:Item"

xpath_vocab_type = f"{voc_root}/cmd:type"
xpath_title = f"({voc_root}/cmd:title[@xml:lang='en'][normalize-space(.)!=''],base-uri(/cmd:CMD)[normalize-space(.)!=''])[1]"
xpath_description = f"{voc_root}/cmd:Description/cmd:description[@xml:lang='en']"
xpath_license = f"{voc_root}/cmd:License/cmd:url"
xpath_publisher = f"{voc_root}/cmd:Assessement/cmd:Recommendation/cmd:Publisher"
xpath_location = f"{voc_root}/cmd:Location"
xpath_version = f"{voc_root}/cmd:Version"
xpath_summary = f"{voc_root}/cmd:Summary"

xpath_summary_ns = f"{voc_root}/cmd:Summary/cmd:Namespace"
xpath_summary_ns_uri = f"{voc_root}/cmd:Summary/cmd:Namespace/cmd:URI"
xpath_summary_ns_prefix = f"{voc_root}/cmd:Summary/cmd:Namespace/cmd:prefix"

xpath_summary_st = f"{voc_root}/cmd:Summary/cmd:Statements"
xpath_summary_st_subj = f"{voc_root}/cmd:Summary/cmd:Statements/cmd:Subjects"
xpath_summary_st_pred = f"{voc_root}/cmd:Summary/cmd:Statements/cmd:Predicates"
xpath_summary_st_obj = f"{voc_root}/cmd:Summary/cmd:Statements/cmd:Objects"
xpath_summary_st_obj_classes = f"{voc_root}/cmd:Summary/cmd:Statements/cmd:Objects/cmd:Classes"
xpath_summary_st_obj_literals = f"{voc_root}/cmd:Summary/cmd:Statements/cmd:Objects/cmd:Literals"
xpath_summary_st_obj_literals_lang = f"{voc_root}/cmd:Summary/cmd:Statements/cmd:Objects/cmd:Literals/cmd:Languages/cmd:Language"


class Location(BaseModel):
    location: str
    type: str
    recipe: Optional[str] = None


class Usage(BaseModel):
    count: int
    outOf: int


class Recommendation(BaseModel):
    publisher: str
    rating: Optional[str] = None


class Version(BaseModel):
    version: str
    validFrom: Optional[datetime] = None
    locations: List[Location]


class Namespace(BaseModel):
    uri: str
    prefix: str


class SummaryNamespaceStats(Namespace):
    count: Optional[int] = None


class SummaryNamespaceNameStats(SummaryNamespaceStats):
    name: str


class SummaryStats(BaseModel):
    count: Optional[int] = None
    stats: List[SummaryNamespaceStats]


class SummaryListStats(SummaryStats):
    list: List[SummaryNamespaceNameStats]


class SummaryListLanguageStats(SummaryListStats):
    languages: dict[str, int]


class SummaryObjectStats(SummaryStats):
    classes: SummaryListStats
    literals: SummaryListLanguageStats


class Summary(BaseModel):
    namespace: Optional[Namespace] = None
    stats: Optional[SummaryStats] = None
    subjects: Optional[SummaryStats] = None
    predicates: Optional[SummaryListStats] = None
    objects: Optional[SummaryObjectStats] = None


class Vocab(BaseModel):
    id: str
    type: str
    title: str
    description: str
    license: str
    versioningPolicy: Optional[str] = None
    sustainabilityPolicy: Optional[str] = None
    created: datetime
    modified: datetime
    locations: List[Location]
    reviews: List[Any] = []
    usage: Usage
    recommendations: List[Recommendation]
    summary: Optional[Summary] = None
    versions: List[Version]


def get_file_for_id(id: str) -> str:
    return os.path.join(records_path, id + '.cmdi')


def read_root(file: str) -> Element:
    parsed = etree.parse(file)
    return parsed.getroot()


def write_root(file: str, root: Element) -> None:
    tree = etree.ElementTree(root)
    etree.indent(tree, space='    ', level=0)
    tree.write(file, encoding='utf-8')


def grab_first(path: str, root: Element) -> Element:
    content = elementpath.select(root, path, ns)
    return content[0] if content else None


def grab_value(path, root, func=None):
    content = elementpath.select(root, path, ns)
    if content and type(content[0]) == str:
        content = unicodedata.normalize("NFKC", content[0]).strip()
    elif content and content[0].text is not None:
        content = unicodedata.normalize("NFKC", content[0].text).strip()
    else:
        content = None

    if content:
        content = cleandoc(content)

    if content and func:
        content = func(content)

    return content


def get_record(id: str) -> Vocab:
    def create_summary_for(elem: Element) -> SummaryStats:
        return SummaryStats(
            count=grab_value(xpath_count, elem, int),
            stats=[SummaryNamespaceStats(
                uri=grab_value(xpath_URI, ns_elem),
                prefix=grab_value(xpath_prefix, ns_elem),
                count=grab_value(xpath_count, ns_elem, int),
            ) for ns_elem in elementpath.select(elem, xpath_namespace_elem, ns)]
        )

    def create_list_for(elem: Element) -> List[SummaryNamespaceNameStats]:
        return [SummaryNamespaceNameStats(
            uri=grab_value(xpath_URI, list_item_elem),
            prefix=grab_value(xpath_prefix, list_item_elem),
            name=grab_value(xpath_name, list_item_elem),
            count=grab_value(xpath_count, list_item_elem, int),
        ) for list_item_elem in elementpath.select(elem, xpath_list_item_elem, ns)]

    def create_location_for(elem: Element) -> Location:
        return Location(
            location=grab_value(xpath_uri, elem),
            type=grab_value(xpath_type, elem),
            recipe=grab_value(xpath_recipe, elem),
        )

    file = get_file_for_id(id)
    root = read_root(file)

    try:
        summary_namespace = Namespace(
            uri=grab_value(xpath_summary_ns_uri, root),
            prefix=grab_value(xpath_summary_ns_prefix, root)
        ) if grab_first(xpath_summary_ns, root) is not None else None

        summary = Summary(
            namespace=summary_namespace,
            stats=create_summary_for(grab_first(xpath_summary, root)),
            subjects=create_summary_for(grab_first(xpath_summary_st_subj, root)),
            predicates=SummaryListStats(
                **create_summary_for(grab_first(xpath_summary_st_pred, root)).model_dump(),
                list=create_list_for(grab_first(xpath_summary_st_pred, root)),
            ),
            objects=SummaryObjectStats(
                **create_summary_for(grab_first(xpath_summary_st_obj, root)).model_dump(),
                classes=SummaryListStats(
                    **create_summary_for(grab_first(xpath_summary_st_obj_classes, root)).model_dump(),
                    list=create_list_for(grab_first(xpath_summary_st_obj_classes, root)),
                ),
                literals=SummaryListLanguageStats(
                    **create_summary_for(grab_first(xpath_summary_st_obj_literals, root)).model_dump(),
                    list=create_list_for(grab_first(xpath_summary_st_obj_literals, root)),
                    languages={
                        grab_value(xpath_code, lang_elem): grab_value(xpath_count, lang_elem, int)
                        for lang_elem in elementpath.select(root, xpath_summary_st_obj_literals_lang, ns)
                    },
                ),
            )
        ) if grab_first(xpath_summary_st, root) is not None else (
            Summary(namespace=summary_namespace)) if summary_namespace is not None else None

        record = Vocab(
            id=id,
            type=grab_value(xpath_vocab_type, root),
            title=grab_value(xpath_title, root),
            description=grab_value(xpath_description, root),
            license=grab_value(xpath_license, root) or 'http://rightsstatements.org/vocab/UND/1.0/',
            versioningPolicy=None,
            sustainabilityPolicy=None,
            created=datetime.utcfromtimestamp(os.path.getctime(file)).isoformat(),
            modified=datetime.utcfromtimestamp(os.path.getmtime(file)).isoformat(),
            locations=[create_location_for(elem)
                       for elem in elementpath.select(root, xpath_location, ns)],
            reviews=[],
            usage=Usage(count=0, outOf=0),
            recommendations=[Recommendation(publisher=grab_value(xpath_name, elem), rating=None)
                             for elem in elementpath.select(root, xpath_publisher, ns)],
            summary=summary,
            versions=sorted([Version(
                version=grab_value(xpath_version_no, elem),
                validFrom=grab_value(xpath_valid_from, elem),
                locations=[create_location_for(loc_elem)
                           for loc_elem in elementpath.select(elem, xpath_location_elem, ns)]
            ) for elem in elementpath.select(root, xpath_version, ns)],
                key=lambda x: (x.validFrom is not None, x.version), reverse=True)
        )
    except Exception as e:
        log.error(f'Cannot parse record with id {id}')
        raise e

    return record


def with_version(id: str) -> Generator[Tuple[Vocab, Version], None, None]:
    record = get_record(id)
    if record and record.versions:
        for version in record.versions:
            yield record, version
    else:
        log.info(f'No record or versions found for {id}!')


def with_version_and_dump(id: str) -> Generator[Tuple[Vocab, Version, str], None, None]:
    for record, version in with_version(id):
        cached_version_path = get_cached_version(id, version.version)
        if cached_version_path is not None:
            yield record, version, cached_version_path


def write_summary_statements(id, data):
    def write_namespaces(root, data, get_count, check_prefix=lambda prefix: True):
        namespaces = etree.SubElement(root, f"{ns_prefix}Namespaces", nsmap=ns)
        for uri, prefix in data['prefixes'].items():
            if check_prefix(prefix):
                namespace = etree.SubElement(namespaces, f"{ns_prefix}Namespace", nsmap=ns)

                uri_elem = etree.SubElement(namespace, f"{ns_prefix}URI", nsmap=ns)
                uri_elem.text = uri

                prefix_elem = etree.SubElement(namespace, f"{ns_prefix}prefix", nsmap=ns)
                prefix_elem.text = prefix

                count = get_count(prefix)
                if count > 0:
                    count_elem = etree.SubElement(namespace, f"{ns_prefix}count", nsmap=ns)
                    count_elem.text = str(count)

    def write_namespace_items(root, data, items):
        namespace_items_elem = etree.SubElement(root, f"{ns_prefix}NamespaceItems", nsmap=ns)
        for uri, count in [(uri, 0) for uri in items] if type(items) is list else items.items():
            prefix, name = uri.split(':', 1)
            if prefix in data['prefixes'].values():
                uri = list(data['prefixes'].keys())[list(data['prefixes'].values()).index(prefix)]

                namespace_item_elem = etree.SubElement(namespace_items_elem, f"{ns_prefix}NamespaceItem", nsmap=ns)

                namespace_item_uri_elem = etree.SubElement(namespace_item_elem, f"{ns_prefix}URI", nsmap=ns)
                namespace_item_uri_elem.text = uri

                namespace_item_prefix_elem = etree.SubElement(namespace_item_elem, f"{ns_prefix}prefix", nsmap=ns)
                namespace_item_prefix_elem.text = prefix

                namespace_item_name_elem = etree.SubElement(namespace_item_elem, f"{ns_prefix}name", nsmap=ns)
                namespace_item_name_elem.text = name

                namespace_item_count_elem = etree.SubElement(namespace_item_elem, f"{ns_prefix}count", nsmap=ns)
                namespace_item_count_elem.text = str(count)

    file = get_file_for_id(id)
    root = read_root(file)
    vocab = grab_first(voc_root, root)

    summary = grab_first("./cmd:Summary", vocab)
    if summary is None:
        summary = etree.SubElement(vocab, f"{ns_prefix}Summary", nsmap=ns)

    statements = grab_first("./cmd:Statements", summary)
    if statements:
        summary.remove(statements)

    namespaces = grab_first("./cmd:Namespaces", summary)
    if statements:
        summary.remove(namespaces)

    statements = etree.SubElement(summary, f"{ns_prefix}Statements", nsmap=ns)
    statements_count = etree.SubElement(statements, f"{ns_prefix}count", nsmap=ns)
    statements_count.text = str(0)

    subjects = etree.SubElement(statements, f"{ns_prefix}Subjects", nsmap=ns)
    subjects_count = etree.SubElement(subjects, f"{ns_prefix}count", nsmap=ns)
    subjects_count.text = str(data['statements']['subjects'])

    predicates = etree.SubElement(statements, f"{ns_prefix}Predicates", nsmap=ns)
    predicates_count = etree.SubElement(predicates, f"{ns_prefix}count", nsmap=ns)
    predicates_count.text = str(data['statements']['predicates'])

    objects = etree.SubElement(statements, f"{ns_prefix}Objects", nsmap=ns)
    objects_count = etree.SubElement(objects, f"{ns_prefix}count", nsmap=ns)
    objects_count.text = str(data['statements']['objects'])

    object_classes = etree.SubElement(objects, f"{ns_prefix}Classes", nsmap=ns)
    object_classes_count = etree.SubElement(object_classes, f"{ns_prefix}count", nsmap=ns)
    object_classes_count.text = str(data['statements']['objects'])

    object_literals = etree.SubElement(objects, f"{ns_prefix}Literals", nsmap=ns)
    object_literals_count = etree.SubElement(object_literals, f"{ns_prefix}count", nsmap=ns)
    object_literals_count.text = str(data['statements']['literals']['count'])

    literal_languages = etree.SubElement(object_literals, f"{ns_prefix}Languages", nsmap=ns)
    for lang, count in data['statements']['literals']['lang'].items():
        literal_language = etree.SubElement(literal_languages, f"{ns_prefix}Language", nsmap=ns)

        literal_language_code = etree.SubElement(literal_language, f"{ns_prefix}code", nsmap=ns)
        literal_language_code.text = lang

        literal_language_count = etree.SubElement(literal_language, f"{ns_prefix}count", nsmap=ns)
        literal_language_count.text = str(count)

    write_namespaces(summary, data, get_count=lambda prefix: data['stats'].get(prefix, 0))
    write_namespaces(subjects, data,
                     get_count=lambda prefix: data['statements']['statements']['subjects'].get(prefix, 0),
                     check_prefix=lambda prefix: prefix in data['statements']['statements']['subjects'])
    write_namespaces(predicates, data,
                     get_count=lambda prefix: data['statements']['statements']['predicates'].get(prefix, 0),
                     check_prefix=lambda prefix: prefix in data['statements']['statements']['predicates'])
    write_namespaces(objects, data,
                     get_count=lambda prefix: data['statements']['statements']['objects'].get(prefix, 0),
                     check_prefix=lambda prefix: prefix in data['statements']['statements']['objects'])

    if 'classes' in data['statements']:
        write_namespaces(object_classes, data,
                         get_count=lambda prefix: data['statements']['classes'].get(prefix, 0),
                         check_prefix=lambda prefix: prefix in data['statements']['classes'])

    write_namespaces(object_literals, data,
                     get_count=lambda prefix: data['statements']['literals']['stats'].get('prefix', 0),
                     check_prefix=lambda prefix: prefix in data['statements']['literals']['stats'])

    # TODO: write_namespace_items(predicates, data, data['statements']['predicates'])

    if 'list_of_classes' in data['statements']:
        write_namespace_items(object_classes, data, data['statements']['list_of_classes'])

    write_namespace_items(object_literals, data, data['statements']['literals']['list'])

    write_root(file, root)


def write_location(id: str, version: str, uri: str, type: str, recipe: str | None) -> None:
    file = get_file_for_id(id)
    root = read_root(file)

    version_elem = grab_first(f"{voc_root}/cmd:Version/cmd:version[text()='{version}']/..", root)
    if version_elem is not None:
        for location in elementpath.select(version_elem, "./cmd:Location", ns):
            if grab_value("./cmd:recipe", location) == recipe:
                version_elem.remove(location)

        location = etree.SubElement(version_elem, f"{ns_prefix}Location", nsmap=ns)

        uri_elem = etree.SubElement(location, f"{ns_prefix}uri", nsmap=ns)
        uri_elem.text = uri

        type_elem = etree.SubElement(location, f"{ns_prefix}type", nsmap=ns)
        type_elem.text = type

        if recipe:
            recipe_elem = etree.SubElement(location, f"{ns_prefix}recipe", nsmap=ns)
            recipe_elem.text = recipe

        write_root(file, root)


def write_summary_namespace(id: str, uri: str, prefix: str) -> None:
    file = get_file_for_id(id)
    root = read_root(file)
    vocab = grab_first(voc_root, root)

    summary = grab_first("./cmd:Summary", vocab)
    if summary is None:
        summary = etree.SubElement(vocab, f"{ns_prefix}Summary", nsmap=ns)

    namespace = grab_first("./cmd:Namespace", summary)
    if namespace is None:
        namespace = etree.SubElement(summary, f"{ns_prefix}Namespace", nsmap=ns)

    uri_elem = grab_first("./cmd:URI", namespace)
    if uri_elem is None:
        uri_elem = etree.SubElement(namespace, f"{ns_prefix}URI", nsmap=ns)

    prefix_elem = grab_first("./cmd:prefix", namespace)
    if prefix_elem is None:
        prefix_elem = etree.SubElement(namespace, f"{ns_prefix}prefix", nsmap=ns)

    uri_elem.text = uri
    prefix_elem.text = prefix

    write_root(file, root)
