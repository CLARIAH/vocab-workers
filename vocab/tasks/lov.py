import logging
import requests

from lxml import etree
from typing import Optional
from pydantic import BaseModel

from vocab.app import celery
from vocab.util.http import session
from vocab.util.xml import read_root, grab_first, voc_root, ns_prefix, ns, write_root, get_file_for_id

log = logging.getLogger(__name__)
lov_api_url = 'https://lov.linkeddata.es/dataset/lov/api/v2/vocabulary/info'


class MinimumVocabInfoLOV(BaseModel):
    homepage: Optional[str] = None
    nsp: Optional[str] = None
    prefix: Optional[str] = None
    uri: Optional[str] = None


@celery.task
def lov(id):
    response = session.get(lov_api_url, params={'vocab': id})
    if response.status_code == requests.codes.ok:
        data = MinimumVocabInfoLOV.model_validate(response.json())
        log.info(f'Work wit vocab {id} results: {data}')

        if data.uri and data.prefix:
            write_summary_namespace(id, data.uri, data.prefix)
    else:
        log.info(f'No vocab {id} results!')


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
