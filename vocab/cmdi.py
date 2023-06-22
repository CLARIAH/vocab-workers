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
    def create_summary_for(elem):
        return {
            "count": grab_value("./cmd:count", elem, int),
            "stats": [{
                "uri": grab_value("./cmd:URI", ns_elem),
                "prefix": grab_value("./cmd:prefix", ns_elem),
                "count": grab_value("./cmd:count", ns_elem, int),
            } for ns_elem in elementpath.select(elem, "./cmd:Namespaces/cmd:Namespace", ns)]
        }

    def create_list_for(elem):
        return {
            "list": [{
                "uri": grab_value("./cmd:URI", list_item_elem),
                "prefix": grab_value("./cmd:prefix", list_item_elem),
                "name": grab_value("./cmd:name", list_item_elem),
                "count": grab_value("./cmd:count", list_item_elem, int),
            } for list_item_elem in elementpath.select(elem, "./cmd:List/cmd:Item", ns)]
        }

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
                "predicates": {
                    **create_summary_for(grab_first(f"{voc_root}/cmd:Summary/cmd:Statements/cmd:Predicates", root)),
                    **create_list_for(grab_first(f"{voc_root}/cmd:Summary/cmd:Statements/cmd:Predicates", root)),
                },
                "objects": {
                    **create_summary_for(grab_first(f"{voc_root}/cmd:Summary/cmd:Statements/cmd:Objects", root)),
                    "classes": {
                        **create_summary_for(
                            grab_first(f"{voc_root}/cmd:Summary/cmd:Statements/cmd:Objects/cmd:Classes", root)),
                        **create_list_for(
                            grab_first(f"{voc_root}/cmd:Summary/cmd:Statements/cmd:Objects/cmd:Classes", root)),
                    },
                    "literals": {
                        **create_summary_for(
                            grab_first(f"{voc_root}/cmd:Summary/cmd:Statements/cmd:Objects/cmd:Literals", root)),
                        **create_list_for(
                            grab_first(f"{voc_root}/cmd:Summary/cmd:Statements/cmd:Objects/cmd:Literals", root)),
                        "languages": [{
                            "name": grab_value("./cmd:code", lang_elem),
                            "count": grab_value("./cmd:count", lang_elem, int),
                        } for lang_elem in
                            elementpath.select(root,
                                               f"{voc_root}/cmd:Summary/cmd:Statements/cmd:Objects/cmd:Literals/cmd:Languages/cmd:Language",
                                               ns)],
                    },
                },
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

    def write_items_list(root, data, items):
        list_elem = etree.SubElement(root, f"{ns_prefix}List", nsmap=ns)
        for uri, count in [(uri, 0) for uri in items] if type(items) is list else items.items():
            prefix, name = uri.split(':', 1)
            if prefix in data['prefixes'].values():
                uri = list(data['prefixes'].keys())[list(data['prefixes'].values()).index(prefix)]

                list_item_elem = etree.SubElement(list_elem, f"{ns_prefix}Item", nsmap=ns)

                list_item_uri_elem = etree.SubElement(list_item_elem, f"{ns_prefix}URI", nsmap=ns)
                list_item_uri_elem.text = uri

                list_item_prefix_elem = etree.SubElement(list_item_elem, f"{ns_prefix}prefix", nsmap=ns)
                list_item_prefix_elem.text = prefix

                list_item_name_elem = etree.SubElement(list_item_elem, f"{ns_prefix}name", nsmap=ns)
                list_item_name_elem.text = name

                list_item_count_elem = etree.SubElement(list_item_elem, f"{ns_prefix}count", nsmap=ns)
                list_item_count_elem.text = str(count)

    file = get_file_for_id(id)
    root = read_root(file)
    vocab = grab_first(voc_root, root)

    summary = grab_first("./cmd:Summary", vocab)
    if summary is not None:
        vocab.remove(summary)

    summary = etree.SubElement(vocab, f"{ns_prefix}Summary", nsmap=ns)

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

    # TODO: write_items_list(predicates, data, data['statements']['predicates'])

    if 'list_of_classes' in data['statements']:
        write_items_list(object_classes, data, data['statements']['list_of_classes'])

    write_items_list(object_literals, data, data['statements']['literals']['list'])

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
