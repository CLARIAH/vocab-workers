import os
import requests
import elementpath
import unicodedata

from lxml import etree
from datetime import datetime
from requests.adapters import HTTPAdapter, Retry

session = requests.Session()
session.mount('https://', HTTPAdapter(max_retries=Retry(total=10, backoff_factor=1)))


def get_record(id):
    ns = {"cmd": "http://www.clarin.eu/cmd/"}
    voc_root = './cmd:Components/cmd:Vocabulary'

    def grab_value(path, root):
        content = elementpath.select(root, path, ns)
        if content and type(content[0]) == str:
            return unicodedata.normalize("NFKD", content[0]).strip()
        elif content and content[0].text is not None:
            return unicodedata.normalize("NFKD", content[0].text).strip()
        else:
            return None

    def create_summary_for(elem, is_obj=False):
        summary = {
            "count": int(grab_value("./cmd:count", elem)),
            "stats": [{
                "uri": grab_value("./cmd:URI", ns_elem),
                "prefix": grab_value("./cmd:prefix", ns_elem),
                "count": int(grab_value("./cmd:count", ns_elem)),
            } for ns_elem in elementpath.select(elem, "./cmd:Namespaces/cmd:Namespace", ns)]
        }

        if is_obj:
            summary.update(
                classes=create_summary_for(elementpath.select(elem, "./cmd:Classes", ns)),
                literals=create_summary_for(elementpath.select(elem, "./cmd:Literals", ns))
            )

        return summary

    file = os.environ.get('RECORDS_PATH', '../data/records/') + id
    parsed = etree.parse(file)
    root = parsed.getroot()

    return {
        "id": id,
        "title": grab_value(
            f"({voc_root}/cmd:title[@xml:lang='en'][normalize-space(.)!=''],base-uri(/cmd:CMD)[normalize-space(.)!=''])[1]",
            root),
        "description": grab_value(f"{voc_root}/cmd:Description/cmd:description[@xml:lang='en']", root),
        "license": grab_value(f"{voc_root}/cmd:License/cmd:url", root) or 'http://rightsstatements.org/vocab/UND/1.0/',
        "versioningPolicy": None,
        "sustainabilityPolicy": None,
        "created": datetime.utcfromtimestamp(os.path.getctime(file)).isoformat(),
        "modified": datetime.utcfromtimestamp(os.path.getmtime(file)).isoformat(),
        "locations": [{
            "location": grab_value("./cmd:uri", elem),
            "type": grab_value("./cmd:type", elem),
            "recipe": None
        } for elem in elementpath.select(root, f"{voc_root}/cmd:Location", ns)],
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
            "stats": create_summary_for(grab_value(f"{voc_root}/cmd:Summary", root)),
            "subjects": create_summary_for(
                elementpath.select(root, f"{voc_root}/cmd:Summary/cmd:Statements/cmd:Subjects", ns)),
            "predicates": create_summary_for(
                elementpath.select(root, f"{voc_root}/cmd:Summary/cmd:Statements/cmd:Predicates", ns)),
            "objects": create_summary_for(
                elementpath.select(root, f"{voc_root}/cmd:Summary/cmd:Statements/cmd:Objects", ns), is_obj=True),
        } if elementpath.select(root, f"{voc_root}/cmd:Summary", ns) else None
    }
