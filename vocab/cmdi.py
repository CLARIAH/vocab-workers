import logging
import elementpath

from lxml import etree
from lxml.etree import Element
from pydantic import BaseModel
from typing import Optional, List, Generator, Tuple

from vocab.util.fs import get_cached_version
from vocab.util.xml import ns, ns_prefix, voc_root, grab_value, grab_first, read_root, write_root, get_file_for_id

log = logging.getLogger(__name__)

xpath_identifier = "./cmd:identifier"
xpath_code = "./cmd:code"
xpath_count = "./cmd:count"
xpath_name = "./cmd:name"
xpath_type = "./cmd:type"
xpath_recipe = "./cmd:recipe"
xpath_URI = "./cmd:URI"
xpath_uri = "./cmd:uri"
xpath_prefix = "./cmd:prefix"
xpath_unesco = "./cmd:unesco"
xpath_nwo = "./cmd:nwo"
xpath_tag = "./cmd:tag"
xpath_version_no = "./cmd:version"
xpath_valid_from = "./cmd:validFrom"
xpath_body = "./cmd:body"
xpath_author = "./cmd:author"
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

xpath_title = f"({voc_root}/cmd:title[@xml:lang='en'][normalize-space(.)!=''],base-uri(/cmd:CMD)[normalize-space(.)!=''])[1]"
xpath_description = f"{voc_root}/cmd:description[@xml:lang='en']"

xpath_type_syntax = f"{voc_root}/cmd:Type/cmd:syntax"
xpath_type_kos = f"{voc_root}/cmd:Type/cmd:kos"
xpath_type_entity = f"{voc_root}/cmd:Type/cmd:entity"

xpath_license_uri = f"{voc_root}/cmd:License/cmd:uri"
xpath_license_label = f"{voc_root}/cmd:License/cmd:label"

xpath_topic_domain = f"{voc_root}/cmd:Topic/cmd:Domain"
xpath_topic_tag = f"{voc_root}/cmd:Topic/cmd:Tag"
xpath_publisher = f"{voc_root}/cmd:Publisher"
xpath_root_namespace = f"{voc_root}/cmd:Namespace"
xpath_root_location = f"{voc_root}/cmd:Location"
xpath_review = f"{voc_root}/cmd:Review"
xpath_version = f"{voc_root}/cmd:Version"


class Type(BaseModel):
    syntax: str
    kos: Optional[str] = None
    entity: Optional[str] = None


class License(BaseModel):
    uri: str
    label: str


class Location(BaseModel):
    location: str
    type: str
    recipe: Optional[str] = None


class Namespace(BaseModel):
    uri: str
    prefix: Optional[str] = None


class Domain(BaseModel):
    unesco: Optional[str] = None
    nwo: Optional[str] = None


class Tag(BaseModel):
    tag: str
    uri: Optional[str] = None


class Topic(BaseModel):
    domain: Optional[Domain] = None
    tags: List[Tag] = []


class Review(BaseModel):
    id: int
    review: str
    rating: int
    likes: int
    dislikes: int


class Publisher(BaseModel):
    identifier: str
    name: str
    uri: str


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
    id: str
    title: str
    description: str
    type: Type
    license: License
    locations: List[Location]
    namespace: Optional[Namespace] = None
    topic: Optional[Topic] = None
    reviews: List[Review] = []
    publishers: List[Publisher] = []
    versions: List[Version]


def get_record(id: str) -> Vocab:
    def create_tag_for(elem: Element) -> Tag:
        return Tag(
            tag=grab_value(xpath_tag, elem),
            uri=grab_value(xpath_uri, elem),
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

    def create_publisher_for(elem: Element) -> Publisher:
        return Publisher(
            identifier=grab_value(xpath_identifier, elem),
            name=grab_value(xpath_name, elem),
            uri=grab_value(xpath_uri, elem),
        )

    def create_review_for(id: int, elem: Element) -> Review:
        return Review(
            id=id,
            review=grab_value(xpath_body, elem),
            rating=grab_value(xpath_rating, elem),
            likes=len(elementpath.select(elem, xpath_like, ns)),
            dislikes=len(elementpath.select(elem, xpath_dislike, ns))
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

    file = get_file_for_id(id)
    root = read_root(file)

    try:
        record = Vocab(
            id=id,
            title=grab_value(xpath_title, root),
            description=grab_value(xpath_description, root),
            type=Type(
                syntax=grab_value(xpath_type_syntax, root),
                kos=grab_value(xpath_type_kos, root),
                entity=grab_value(xpath_type_entity, root)
            ),
            license=License(
                uri=grab_value(xpath_license_uri, root) or 'http://rightsstatements.org/vocab/UND/1.0/',
                label=grab_value(xpath_license_label, root) or 'Unknown'
            ),
            namespace=Namespace(
                uri=grab_value(xpath_uri, elementpath.select(root, xpath_root_namespace, ns)[0]),
                prefix=grab_value(xpath_prefix, elementpath.select(root, xpath_root_namespace, ns)[0])
            ) if elementpath.select(root, xpath_root_namespace, ns) else None,
            topic=Topic(
                domain=Domain(
                    unesco=grab_value(xpath_unesco, elementpath.select(root, xpath_topic_domain, ns)),
                    nwo=grab_value(xpath_nwo, elementpath.select(root, xpath_topic_domain, ns))
                ) if elementpath.select(root, xpath_topic_domain, ns) else None,
                tags=[create_tag_for(elem)
                      for elem in elementpath.select(root, xpath_topic_tag, ns)]
            ) if elementpath.select(root, xpath_topic_domain, ns)
                 or elementpath.select(root, xpath_topic_tag, ns) else None,
            locations=[create_location_for(elem)
                       for elem in elementpath.select(root, xpath_root_location, ns)],
            reviews=[create_review_for(i + 1, elem)
                     for i, elem in enumerate(elementpath.select(root, xpath_review, ns))
                     if grab_value(xpath_status, elem) == 'published'],
            publishers=[create_publisher_for(elem)
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
