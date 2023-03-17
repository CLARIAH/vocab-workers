import os
import requests
import elementpath
import unicodedata

from lxml import etree
from requests.adapters import HTTPAdapter, Retry

session = requests.Session()
session.mount('https://', HTTPAdapter(max_retries=Retry(total=10, backoff_factor=1)))


def get_record(rec):
    def grab_value(path):
        content = elementpath.select(root, path, {"cmd": "http://www.clarin.eu/cmd/"})
        if content and type(content[0]) == str:
            return unicodedata.normalize("NFKD", content[0]).strip()
        elif content and content[0].text is not None:
            return unicodedata.normalize("NFKD", content[0].text).strip()
        else:
            return ""

    root = etree.parse(os.environ.get('RECORDS_PATH', '/data/records/') + rec).getroot()
    return {
        "record": rec,
        "title": grab_value(
            "(//cmd:Components/cmd:Vocabulary/cmd:title[@xml:lang='en'][normalize-space(.)!=''],base-uri(/cmd:CMD)[normalize-space(.)!=''],'Hallo Wereld!')[1]"),
        "description": grab_value("./cmd:Components/cmd:Vocabulary/cmd:Description/cmd:description[@xml:lang='en']"),
        "home": grab_value("./cmd:Components/cmd:Vocabulary/cmd:Location[cmd:type='homepage']/cmd:uri"),
        "endpoint": grab_value("./cmd:Components/cmd:Vocabulary/cmd:Location[cmd:type='endpoint']/cmd:uri"),
        "license": grab_value(
            "(./cmd:Components/cmd:Vocabulary/cmd:License/cmd:url,'http://rightsstatements.org/vocab/UND/1.0/')[normalize-space(.)!=''][1]")
    }
