import os
import logging
import operator

import elementpath
import unicodedata

from lxml import etree
from datetime import datetime

log = logging.getLogger(__name__)

ns = {"cmd": "http://www.clarin.eu/cmd/"}
ns_prefix = '{http://www.clarin.eu/cmd/}'
voc_root = './cmd:Components/cmd:Vocabulary'


def get_file_for_id(id):
    return os.environ.get('RECORDS_PATH', '../data/records/') + id + '.cmdi'


def read_root(file):
    parsed = etree.parse(file)
    return parsed.getroot()


def write_root(file, root):
    tree = etree.ElementTree(root)
    etree.indent(tree, space='    ', level=0)
    tree.write(file, encoding='utf-8')


def grab_first(path, root):
    content = elementpath.select(root, path, ns)
    return content[0] if content else None


def grab_value(path, root, func=None):
    content = elementpath.select(root, path, ns)
    if content and type(content[0]) == str:
        content = unicodedata.normalize("NFKD", content[0]).strip()
    elif content and content[0].text is not None:
        content = unicodedata.normalize("NFKD", content[0].text).strip()
    else:
        content = None

    if content and func:
        content = func(content)

    return content


def get_record(id):
    def create_summary_for(elem, is_obj=False):
        summary = {
            "count": grab_value("./cmd:count", elem, int),
            "stats": [{
                "uri": grab_value("./cmd:URI", ns_elem),
                "prefix": grab_value("./cmd:prefix", ns_elem),
                "count": grab_value("./cmd:count", ns_elem, int),
            } for ns_elem in elementpath.select(elem, "./cmd:Namespaces/cmd:Namespace", ns)]
        }

        if is_obj:
            classes_root = grab_first("./cmd:Classes", elem)
            literals_root = grab_first("./cmd:Literals", elem)

            summary.update(
                classes=create_summary_for(classes_root) if classes_root else None,
                literals=create_summary_for(literals_root) if literals_root else None
            )

        return summary

    def create_location_for(elem):
        return {
            "location": grab_value("./cmd:uri", elem),
            "type": grab_value("./cmd:type", elem),
            "recipe": grab_value("./cmd:recipe", elem),
        }

    file = get_file_for_id(id)
    root = read_root(file)

    try:
        record = {
            "id": id,
            "title": grab_value(
                f"({voc_root}/cmd:title[@xml:lang='en'][normalize-space(.)!=''],base-uri(/cmd:CMD)[normalize-space(.)!=''])[1]",
                root),
            "description": grab_value(f"{voc_root}/cmd:Description/cmd:description[@xml:lang='en']", root),
            "license": grab_value(f"{voc_root}/cmd:License/cmd:url",
                                  root) or 'http://rightsstatements.org/vocab/UND/1.0/',
            "versioningPolicy": None,
            "sustainabilityPolicy": None,
            "created": datetime.utcfromtimestamp(os.path.getctime(file)).isoformat(),
            "modified": datetime.utcfromtimestamp(os.path.getmtime(file)).isoformat(),
            "locations": [create_location_for(elem) for elem in
                          elementpath.select(root, f"{voc_root}/cmd:Location", ns)],
            "reviews": [],
            "usage": {
                "count": 0,
                "outOf": 0
            },
            "recommendations": [{
                "publisher": grab_value("./cmd:name", elem),
                "rating": None
            } for elem in elementpath.select(root, f"{voc_root}/cmd:Assessement/cmd:Recommendation/cmd:Publisher", ns)],
            "summary": {
                "namespace": {
                    "uri": grab_value(f"{voc_root}/cmd:Summary/cmd:Namespace/cmd:URI", root),
                    "prefix": grab_value(f"{voc_root}/cmd:Summary/cmd:Namespace/cmd:prefix", root)
                },
                "stats": create_summary_for(grab_first(f"{voc_root}/cmd:Summary", root)),
                "subjects": create_summary_for(grab_first(f"{voc_root}/cmd:Summary/cmd:Statements/cmd:Subjects", root)),
                "predicates": create_summary_for(
                    grab_first(f"{voc_root}/cmd:Summary/cmd:Statements/cmd:Predicates", root)),
                "objects": create_summary_for(grab_first(f"{voc_root}/cmd:Summary/cmd:Statements/cmd:Objects", root),
                                              is_obj=True),
            } if grab_first(f"{voc_root}/cmd:Summary", root) is not None else None,
            "versions": sorted([{
                "version": grab_value("./cmd:version", elem),
                "validFrom": grab_value("./cmd:validFrom", elem),
                "locations": [create_location_for(loc_elem) for loc_elem in
                              elementpath.select(elem, "./cmd:Location", ns)],
            } for elem in elementpath.select(root, f"{voc_root}/cmd:Version", ns)],
                key=operator.itemgetter('validFrom', 'version'), reverse=True)
        }
    except Exception as e:
        log.error(f'Cannot parse record with id {id}')
        raise e

    return record


def write_summary(id, data):
    file = get_file_for_id(id)
    root = read_root(file)
    vocab = grab_first(voc_root, root)

    summary = grab_first("./cmd:Summary", vocab)
    if summary is not None:
        vocab.remove(summary)

    summary = etree.SubElement(vocab, f"{ns_prefix}Summary", nsmap=ns)
    statements = etree.SubElement(summary, f"{ns_prefix}Statements", nsmap=ns)

    subjects = etree.SubElement(statements, f"{ns_prefix}Subjects", nsmap=ns)
    subjects_count = etree.SubElement(subjects, f"{ns_prefix}count", nsmap=ns)
    subjects_count.text = str(data['statements']['unique subjects'])

    predicates = etree.SubElement(statements, f"{ns_prefix}Predicates", nsmap=ns)
    predicates_count = etree.SubElement(predicates, f"{ns_prefix}count", nsmap=ns)
    predicates_count.text = str(data['statements']['unique predicates'])

    objects = etree.SubElement(statements, f"{ns_prefix}Objects", nsmap=ns)
    objects_count = etree.SubElement(objects, f"{ns_prefix}count", nsmap=ns)
    objects_count.text = str(data['statements']['unique objects'])

    namespaces = etree.SubElement(summary, f"{ns_prefix}Namespaces", nsmap=ns)
    for uri, prefix in data['prefixes'].items():
        namespace = etree.SubElement(namespaces, f"{ns_prefix}Namespace", nsmap=ns)

        uri_elem = etree.SubElement(namespace, f"{ns_prefix}URI", nsmap=ns)
        uri_elem.text = uri

        prefix_elem = etree.SubElement(namespace, f"{ns_prefix}prefix", nsmap=ns)
        prefix_elem.text = prefix

        if prefix in data['stats']:
            count_elem = etree.SubElement(namespace, f"{ns_prefix}count", nsmap=ns)
            count_elem.text = str(data['stats'][prefix])

    write_root(file, root)


def write_location(id, uri, type, recipe):
    file = get_file_for_id(id)
    root = read_root(file)
    vocab = grab_first(voc_root, root)

    for location in elementpath.select(root, "./cmd:Location", ns):
        if grab_value("./cmd:uri", location) == uri:
            vocab.remove(location)

    location = etree.SubElement(vocab, f"{ns_prefix}Location", nsmap=ns)

    uri_elem = etree.SubElement(location, f"{ns_prefix}uri", nsmap=ns)
    uri_elem.text = uri

    type_elem = etree.SubElement(location, f"{ns_prefix}type", nsmap=ns)
    type_elem.text = type

    if recipe:
        recipe_elem = etree.SubElement(location, f"{ns_prefix}recipe", nsmap=ns)
        recipe_elem.text = recipe

    write_root(file, root)
