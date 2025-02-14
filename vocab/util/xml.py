import unicodedata
import elementpath

from lxml import etree
from lxml.etree import Element
from inspect import cleandoc

ns = {"cmd": "http://www.clarin.eu/cmd/"}
ns_prefix = '{http://www.clarin.eu/cmd/}'
voc_root = './cmd:Components/cmd:Vocabulary'


def read_xml(xml: bytes) -> Element:
    return etree.fromstring(xml)


def write_xml(xml: Element, pretty: bool = False) -> bytes:
    if pretty:
        etree.indent(xml, space='    ', level=0)
    return etree.tostring(xml, encoding='utf-8')


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
