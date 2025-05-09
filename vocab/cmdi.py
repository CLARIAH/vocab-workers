import logging
from contextlib import contextmanager

import elementpath

from lxml import etree
from lxml.etree import Element
from datetime import datetime
from pydantic import BaseModel
from typing import Optional, List, Generator, Tuple, Any

from vocab.util.fs import get_cached_version
from vocab.util.redis import get_object_redis, store_object_redis
from vocab.util.xml import ns, ns_prefix, voc_root, grab_value, grab_first, read_xml, write_xml

log = logging.getLogger(__name__)

xpath_identifier = "./cmd:identifier"
xpath_code = "./cmd:code"
xpath_count = "./cmd:count"
xpath_name = "./cmd:name"
xpath_type = "./cmd:type"
xpath_recipe = "./cmd:recipe"
xpath_URI = "./cmd:URI"
xpath_uri = "./cmd:uri"
xpath_label = "./cmd:label"
xpath_title = "./cmd:title"
xpath_url = "./cmd:url"
xpath_landing_page = "./cmd:landingPage"
xpath_prefix = "./cmd:prefix"
xpath_version_no = "./cmd:version"
xpath_valid_from = "./cmd:validFrom"
xpath_body = "./cmd:body"
xpath_author = "./cmd:author"
xpath_published = "./cmd:published"
xpath_status = "./cmd:status"
xpath_rating = "./cmd:rating"
xpath_like = "./cmd:like"
xpath_dislike = "./cmd:dislike"

xpath_location = "./cmd:Location"
xpath_namespace = "./cmd:Namespace"
xpath_summary = "./cmd:Summary"
xpath_namespace_item = "./cmd:NamespaceItems/cmd:NamespaceItem"

xpath_summary_ns = "./cmd:Summary/cmd:Namespace"
xpath_summary_ns_uri = "./cmd:Summary/cmd:Namespace/cmd:URI"
xpath_summary_ns_prefix = "./cmd:Summary/cmd:Namespace/cmd:prefix"

xpath_summary_st = "./cmd:Summary/cmd:Statements"
xpath_summary_st_subj = "./cmd:Summary/cmd:Statements/cmd:Subjects"
xpath_summary_st_pred = "./cmd:Summary/cmd:Statements/cmd:Predicates"
xpath_summary_st_obj = "./cmd:Summary/cmd:Statements/cmd:Objects"
xpath_summary_st_obj_classes = "./cmd:Summary/cmd:Statements/cmd:Objects/cmd:Classes"
xpath_summary_st_obj_literals = "./cmd:Summary/cmd:Statements/cmd:Objects/cmd:Literals"
xpath_summary_st_obj_literals_lang = "./cmd:Summary/cmd:Statements/cmd:Objects/cmd:Literals/cmd:Languages/cmd:Language"

xpath_identification_identifier = f"{voc_root}/cmd:Identification/cmd:identifier"
xpath_identification_title = f"{voc_root}/cmd:Identification/cmd:title"
xpath_identification_namespace = f"{voc_root}/cmd:Identification/cmd:Namespace"
xpath_responsibility_creators = f"{voc_root}/cmd:Responsibility/cmd:Creator"
xpath_responsibility_maintainers = f"{voc_root}/cmd:Responsibility/cmd:Maintainer"
xpath_responsibility_contributors = f"{voc_root}/cmd:Responsibility/cmd:Contributor"
xpath_description_description = f"{voc_root}/cmd:Description/cmd:description"
xpath_description_date_issued = f"{voc_root}/cmd:Description/cmd:dateIssued"
xpath_description_languages = f"{voc_root}/cmd:Description/cmd:language"
xpath_description_topic_unesco = f"{voc_root}/cmd:Description/cmd:topicUnesco"
xpath_description_topic_nwo = f"{voc_root}/cmd:Description/cmd:topicNwo"
xpath_description_keywords = f"{voc_root}/cmd:Description/cmd:Keywords"
xpath_licenses = f"{voc_root}/cmd:License"
xpath_is_referenced_by_registries = f"{voc_root}/cmd:IsReferencedBy/cmd:Registry"
xpath_locations = f"{voc_root}/cmd:Location"
xpath_version = f"{voc_root}/cmd:Version"
xpath_review = f"{voc_root}/cmd:Review"
xpath_type_syntax = f"{voc_root}/cmd:Type/cmd:syntax"
xpath_type_kos = f"{voc_root}/cmd:Type/cmd:kos"
xpath_type_entity = f"{voc_root}/cmd:Type/cmd:entity"
xpath_license_uri = f"{voc_root}/cmd:License/cmd:uri"
xpath_license_label = f"{voc_root}/cmd:License/cmd:label"
xpath_topic_domain = f"{voc_root}/cmd:Topic/cmd:Domain"
xpath_topic_tag = f"{voc_root}/cmd:Topic/cmd:Tag"
xpath_publisher = f"{voc_root}/cmd:Publisher"


class Authority(BaseModel):
    uri: Optional[str] = None
    label: str


class Type(BaseModel):
    syntax: str
    kos: Optional[str] = None
    entity: Optional[str] = None


class Registry(BaseModel):
    title: str
    url: str
    landing_page: Optional[str] = None


class Topic(BaseModel):
    unesco: Optional[str] = None
    nwo: Optional[str] = None


class Location(BaseModel):
    location: str
    type: str
    recipe: Optional[str] = None


class Namespace(BaseModel):
    uri: str
    prefix: Optional[str] = None


class Review(BaseModel):
    id: int
    status: str
    author: str
    published: datetime
    body: str
    rating: int
    likes: List[str] = []
    dislikes: List[str] = []


class SummaryNamespaceStats(Namespace):
    count: Optional[int] = 0


class SummaryNamespaceNameStats(SummaryNamespaceStats):
    name: str


class SummaryStats(BaseModel):
    count: Optional[int] = 0
    stats: List[SummaryNamespaceStats]


class SummaryListStats(SummaryStats):
    list: List[SummaryNamespaceNameStats]


class SummaryListLanguageStats(SummaryListStats):
    languages: dict[str, int]


class SummaryObjectStats(SummaryStats):
    classes: SummaryListStats
    literals: SummaryListLanguageStats


class Summary(BaseModel):
    stats: Optional[SummaryStats] = None
    subjects: Optional[SummaryStats] = None
    predicates: Optional[SummaryStats] = None
    objects: Optional[SummaryObjectStats] = None


class Version(BaseModel):
    version: str
    validFrom: Optional[str] = None
    locations: List[Location] = []
    summary: Optional[Summary] = None


class Vocab(BaseModel):
    identifier: str
    title: str
    namespace: Optional[Namespace] = None
    creators: List[Authority] = []
    maintainers: List[Authority] = []
    contributors: List[Authority] = []
    description: str
    date_issued: Optional[datetime] = None
    languages: List[str] = []
    topic: Optional[Topic] = None
    keywords: List[Authority] = []
    type: Type
    licenses: List[Authority] = []
    registries: List[Registry] = []
    locations: List[Location]
    versions: List[Version]
    reviews: List[Review] = []


def get_record(nr: int, id: int) -> Vocab:
    def create_authority_for(elem: Element) -> Authority:
        return Authority(
            uri=grab_value(xpath_uri, elem),
            label=grab_value(xpath_label, elem),
        )

    def create_relaxing_authority_for(elem: Element) -> Authority:
        return Authority(
            uri=grab_value(xpath_uri, elem),
            label=grab_value(xpath_label, elem),
        )

    def create_registry_for(elem: Element) -> Registry:
        return Registry(
            title=grab_value(xpath_title, elem),
            url=grab_value(xpath_url, elem),
            landing_page=grab_value(xpath_landing_page, elem),
        )

    def create_summary_for(elem: Element) -> SummaryStats:
        return SummaryStats(
            count=grab_value(xpath_count, elem, int),
            stats=[SummaryNamespaceStats(
                uri=grab_value(xpath_URI, ns_elem),
                prefix=grab_value(xpath_prefix, ns_elem),
                count=grab_value(xpath_count, ns_elem, int),
            ) for ns_elem in elementpath.select(elem, xpath_namespace, ns)]
        )

    def create_list_for(elem: Element) -> List[SummaryNamespaceNameStats]:
        return [SummaryNamespaceNameStats(
            uri=grab_value(xpath_URI, list_item_elem),
            prefix=grab_value(xpath_prefix, list_item_elem),
            name=grab_value(xpath_name, list_item_elem),
            count=grab_value(xpath_count, list_item_elem, int),
        ) for list_item_elem in elementpath.select(elem, xpath_namespace_item, ns)]

    def create_location_for(elem: Element) -> Location:
        return Location(
            location=grab_value(xpath_uri, elem),
            type=grab_value(xpath_type, elem),
            recipe=grab_value(xpath_recipe, elem),
        )

    def create_version(elem: Element) -> Version:
        summary = Summary(
            stats=create_summary_for(grab_first(xpath_summary, elem)),
            subjects=create_summary_for(grab_first(xpath_summary_st_subj, elem)),
            predicates=create_summary_for(grab_first(xpath_summary_st_pred, elem)),
            objects=SummaryObjectStats(
                **create_summary_for(grab_first(xpath_summary_st_obj, elem)).model_dump(),
                classes=SummaryListStats(
                    **create_summary_for(grab_first(xpath_summary_st_obj_classes, elem)).model_dump(),
                    list=create_list_for(grab_first(xpath_summary_st_obj_classes, elem)),
                ),
                literals=SummaryListLanguageStats(
                    **create_summary_for(grab_first(xpath_summary_st_obj_literals, elem)).model_dump(),
                    list=create_list_for(grab_first(xpath_summary_st_obj_literals, elem)),
                    languages={
                        grab_value(xpath_code, lang_elem): grab_value(xpath_count, lang_elem, int)
                        for lang_elem in elementpath.select(elem, xpath_summary_st_obj_literals_lang, ns)
                    },
                ),
            )
        ) if grab_first(xpath_summary_st, elem) is not None else None

        return Version(
            version=grab_value(xpath_version_no, elem),
            validFrom=grab_value(xpath_valid_from, elem),
            locations=[create_location_for(loc_elem)
                       for loc_elem in elementpath.select(elem, xpath_location, ns)],
            summary=summary
        )

    def create_review_for(id: int, elem: Element) -> Review:
        return Review(
            id=id,
            status=grab_value(xpath_status, elem),
            author=grab_value(xpath_author, elem),
            published=grab_value(xpath_published, elem),
            body=grab_value(xpath_body, elem),
            rating=grab_value(xpath_rating, elem),
            likes=[grab_value('.', elem)
                   for elem in elementpath.select(root, xpath_like, ns)],
            dislikes=[grab_value('.', elem)
                      for elem in elementpath.select(root, xpath_dislike, ns)],
        )

    xml_bytes = get_object_redis(nr, id)
    root = read_xml(xml_bytes)

    try:
        record = Vocab(
            identifier=grab_value(xpath_identification_identifier, root),
            title=grab_value(xpath_identification_title, root),
            namespace=Namespace(
                uri=grab_value(xpath_uri, grab_first(xpath_identification_namespace, root)),
                prefix=grab_value(xpath_prefix, grab_first(xpath_identification_namespace, root))
            ) if grab_first(xpath_identification_namespace, root) is not None else None,
            creators=[create_authority_for(elem)
                      for elem in elementpath.select(root, xpath_responsibility_creators, ns)],
            maintainers=[create_authority_for(elem)
                         for elem in elementpath.select(root, xpath_responsibility_maintainers, ns)],
            contributors=[create_authority_for(elem)
                          for elem in elementpath.select(root, xpath_responsibility_contributors, ns)],
            description=grab_value(xpath_description_description, root),
            date_issued=grab_value(xpath_description_date_issued, root),
            languages=[grab_value('.', elem)
                       for elem in elementpath.select(root, xpath_description_languages, ns)],
            topic=Topic(
                unesco=grab_value(xpath_description_topic_unesco, root),
                nwo=grab_value(xpath_description_topic_nwo, root)
            ) if grab_first(xpath_description_topic_unesco, root) is not None or
                 grab_first(xpath_description_topic_nwo, root) is not None else None,
            keywords=[create_relaxing_authority_for(elem)
                      for elem in elementpath.select(root, xpath_description_keywords, ns)],
            type=Type(
                syntax=grab_value(xpath_type_syntax, root),
                kos=grab_value(xpath_type_kos, root),
                entity=grab_value(xpath_type_entity, root)
            ),
            licenses=[create_relaxing_authority_for(elem)
                      for elem in elementpath.select(root, xpath_licenses, ns)],
            registries=[create_registry_for(elem)
                        for elem in elementpath.select(root, xpath_is_referenced_by_registries, ns)],
            locations=[create_location_for(elem)
                       for elem in elementpath.select(root, xpath_locations, ns)],
            versions=sorted([create_version(elem) for elem in elementpath.select(root, xpath_version, ns)],
                            key=lambda x: (x.validFrom is not None, x.version), reverse=True),
            reviews=[create_review_for(i + 1, elem)
                     for i, elem in enumerate(elementpath.select(root, xpath_review, ns))]
        )
    except Exception as e:
        log.error(f'Cannot parse record nr {nr}')
        raise e

    return record


def with_version(nr: int, id: int) -> Generator[Tuple[Vocab, Version], None, None]:
    record = get_record(nr, id)
    if record and record.versions:
        for version in record.versions:
            yield record, version
    else:
        log.info(f'No record or versions found for {id}!')


def with_version_and_dump(nr: int, id: int) -> Generator[Tuple[Vocab, Version, str], None, None]:
    for record, version in with_version(nr, id):
        cached_version_path = get_cached_version(record.identifier, version.version)
        if cached_version_path is not None:
            yield record, version, cached_version_path


@contextmanager
def cmdi_from_redis(nr: int, id: int) -> Generator[etree.Element, None, None]:
    xml_bytes = get_object_redis(nr, id)
    xml = read_xml(xml_bytes)

    yield xml

    xml_bytes = write_xml(xml)
    store_object_redis(nr, id, xml_bytes)


def write_location(nr: int, id: int, version: str, uri: str, type: str, recipe: str | None) -> None:
    with cmdi_from_redis(nr, id) as xml:
        version_elem = grab_first(f"{voc_root}/cmd:Version/cmd:version[text()='{version}']/..", xml)
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


def write_registry(nr: int, id: int, title: str, url: str, landing_page: str | None) -> None:
    with (cmdi_from_redis(nr, id) as xml):
        is_referenced_by_elem = grab_first(f"{voc_root}/cmd:IsReferencedBy", xml)
        if is_referenced_by_elem is None:
            is_referenced_by_elem = etree.SubElement(grab_first(f"{voc_root}", xml), f"{ns_prefix}IsReferencedBy",
                                                     nsmap=ns)

        registry = grab_first(f"./cmd:Registry/cmd:url[text()='{url}']/..", is_referenced_by_elem)
        if registry is not None:
            is_referenced_by_elem.remove(registry)

        registry = etree.SubElement(is_referenced_by_elem, f"{ns_prefix}Registry", nsmap=ns)

        title_elem = etree.SubElement(registry, f"{ns_prefix}title", nsmap=ns)
        title_elem.text = title

        url_elem = etree.SubElement(registry, f"{ns_prefix}url", nsmap=ns)
        url_elem.text = url

        if landing_page is not None:
            landing_page_elem = etree.SubElement(registry, f"{ns_prefix}landingPage", nsmap=ns)
            landing_page_elem.text = landing_page
