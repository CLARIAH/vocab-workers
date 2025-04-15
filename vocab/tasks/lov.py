import sys
import logging
import requests

from lxml import etree
from typing import Optional
from pydantic import BaseModel

from vocab.app import celery
from vocab.cmdi import get_record, cmdi_from_redis, write_registry
from vocab.util.http import session
from vocab.util.file import get_files_in_path, run_work_for_file
from vocab.util.xml import grab_first, ns_prefix, ns, voc_root

log = logging.getLogger(__name__)
lov_api_url = 'https://lov.linkeddata.es/dataset/lov/api/v2/vocabulary/info'


class MinimumVocabInfoLOV(BaseModel):
    nsp: Optional[str] = None
    prefix: Optional[str] = None


@celery.task(name='rdf.lov', autoretry_for=(Exception,),
             default_retry_delay=60 * 30, retry_kwargs={'max_retries': 5})
def lov(nr: int, id: int) -> None:
    record = get_record(nr, id)
    if record and record.type.syntax in ['owl', 'skos', 'rdfs']:
        response = session.get(lov_api_url, params={'vocab': record.identifier})
        if response.status_code == requests.codes.ok:
            data = MinimumVocabInfoLOV.model_validate(response.json())
            log.info(f'Work wit vocab {record.identifier} results: {data}')

            write_registry(nr, id, "LOV", "https://lov.linkeddata.es",
                           f"https://lov.linkeddata.es/dataset/lov/vocabs/{record.identifier}")
            if data.nsp and data.prefix:
                write_namespace(nr, id, data.nsp, data.prefix)
        else:
            log.info(f'No vocab {record.identifier} results!')


def write_namespace(nr: int, id: int, uri: str, prefix: str) -> None:
    with cmdi_from_redis(nr, id) as vocab:
        namespace = grab_first(f"{voc_root}/cmd:Identification/cmd:Namespace", vocab)
        if namespace is None:
            identification = grab_first("./cmd:Identification", vocab)
            namespace = etree.SubElement(identification, f"{ns_prefix}Namespace", nsmap=ns)

        uri_elem = grab_first("./cmd:uri", namespace)
        if uri_elem is None:
            uri_elem = etree.SubElement(namespace, f"{ns_prefix}uri", nsmap=ns)

        prefix_elem = grab_first("./cmd:prefix", namespace)
        if prefix_elem is None:
            prefix_elem = etree.SubElement(namespace, f"{ns_prefix}prefix", nsmap=ns)

        uri_elem.text = uri
        prefix_elem.text = prefix


if __name__ == '__main__':
    for f in get_files_in_path(sys.argv[1]):
        with run_work_for_file(f) as (nr, id):
            lov(nr, id)
