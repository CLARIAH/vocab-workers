import os
import logging
import elementpath

from lxml import etree
from lxml.etree import Element
from pydantic import BaseModel
from datetime import datetime, UTC
from typing import Optional, List, Generator, Tuple

from vocab.util.fs import get_cached_version
from vocab.util.xml import ns, ns_prefix, voc_root, grab_value, grab_first, read_root, write_root, get_file_for_id

log = logging.getLogger(__name__)

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
xpath_review_elem = "./cmd:body"
xpath_review_author_elem = "./cmd:author"
xpath_review_status_elem = "./cmd:status"
xpath_rating_elem = "./cmd:rating"
xpath_like_elem = "./cmd:like"
xpath_dislike_elem = "./cmd:dislike"
xpath_namespace_elem = "./cmd:Namespaces/cmd:Namespace"
xpath_namespace_item_elem = "./cmd:NamespaceItems/cmd:NamespaceItem"
xpath_summary_elem = "./cmd:Summary"

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

xpath_vocab_type = f"{voc_root}/cmd:type"
xpath_title = f"({voc_root}/cmd:title[@xml:lang='en'][normalize-space(.)!=''],base-uri(/cmd:CMD)[normalize-space(.)!=''])[1]"
xpath_description = f"{voc_root}/cmd:Description/cmd:description"
xpath_license = f"{voc_root}/cmd:License/cmd:url"
xpath_publisher = f"{voc_root}/cmd:Assessement/cmd:Recommendation/cmd:Publisher"
xpath_review = f"{voc_root}/cmd:Assessement/cmd:Review"
xpath_location = f"{voc_root}/cmd:Location"
xpath_version = f"{voc_root}/cmd:Version"
xpath_assessment = f"{voc_root}/cmd:Assessement"


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


class Review(BaseModel):
    id: int
    review: str
    rating: int
    likes: int
    dislikes: int


class Namespace(BaseModel):
    uri: str
    prefix: Optional[str] = None


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
    namespace: Optional[Namespace] = None
    stats: Optional[SummaryStats] = None
    subjects: Optional[SummaryStats] = None
    predicates: Optional[SummaryStats] = None
    objects: Optional[SummaryObjectStats] = None


class Version(BaseModel):
    version: str
    validFrom: Optional[str] = None
    locations: List[Location]
    summary: Optional[Summary] = None


class Vocab(BaseModel):
    id: str
    type: str
    title: str
    description: str
    license: str
    created: datetime
    modified: datetime
    locations: List[Location]
    reviews: List[Review] = []
    usage: Usage
    recommendations: List[Recommendation]
    versions: List[Version]


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
        ) for list_item_elem in elementpath.select(elem, xpath_namespace_item_elem, ns)]

    def create_location_for(elem: Element) -> Location:
        return Location(
            location=grab_value(xpath_uri, elem),
            type=grab_value(xpath_type, elem),
            recipe=grab_value(xpath_recipe, elem),
        )

    def create_review_for(id: int, elem: Element) -> Review:
        return Review(
            id=id,
            review=grab_value(xpath_review_elem, elem),
            rating=grab_value(xpath_rating_elem, elem),
            likes=len(elementpath.select(elem, xpath_like_elem, ns)),
            dislikes=len(elementpath.select(elem, xpath_dislike_elem, ns))
        )

    def create_version(elem: Element) -> Version:
        summary_namespace = Namespace(
            uri=grab_value(xpath_summary_ns_uri, elem),
            prefix=grab_value(xpath_summary_ns_prefix, elem)
        ) if grab_first(xpath_summary_ns, elem) is not None else None

        summary = Summary(
            namespace=summary_namespace,
            stats=create_summary_for(grab_first(xpath_summary_elem, elem)),
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
        ) if grab_first(xpath_summary_st, elem) is not None else (
            Summary(namespace=summary_namespace)) if summary_namespace is not None else None

        return Version(
            version=grab_value(xpath_version_no, elem),
            validFrom=grab_value(xpath_valid_from, elem),
            locations=[create_location_for(loc_elem)
                       for loc_elem in elementpath.select(elem, xpath_location_elem, ns)],
            summary=summary
        )

    file = get_file_for_id(id)
    root = read_root(file)

    try:
        record = Vocab(
            id=id,
            type=grab_value(xpath_vocab_type, root),
            title=grab_value(xpath_title, root),
            description=grab_value(xpath_description, root),
            license=grab_value(xpath_license, root) or 'http://rightsstatements.org/vocab/UND/1.0/',
            created=datetime.fromtimestamp(os.path.getctime(file), UTC).isoformat(),
            modified=datetime.fromtimestamp(os.path.getmtime(file), UTC).isoformat(),
            locations=[create_location_for(elem)
                       for elem in elementpath.select(root, xpath_location, ns)],
            reviews=[create_review_for(i + 1, elem)
                     for i, elem in enumerate(elementpath.select(root, xpath_review, ns))
                     if grab_value(xpath_review_status_elem, elem) == 'published'],
            usage=Usage(count=0, outOf=0),
            recommendations=[Recommendation(publisher=grab_value(xpath_name, elem), rating=None)
                             for elem in elementpath.select(root, xpath_publisher, ns)],
            versions=sorted([create_version(elem) for elem in elementpath.select(root, xpath_version, ns)],
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
