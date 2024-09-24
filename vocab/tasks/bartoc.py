import logging
import os
import requests
from lxml import etree
from typing import Optional
from pydantic import BaseModel
from vocab.app import celery
from vocab.util.http import session
from vocab.util.xml import read_root, grab_first, voc_root, ns_prefix, ns, write_root, get_file_for_id
from vocab.cmdi import get_record

log = logging.getLogger(__name__)
bartoc_api_suggest = 'https://bartoc.org/api/voc/suggest'
bartoc_api_voc = 'https://bartoc.org/api/data'
data_directory = os.path.abspath(os.path.join('..', 'vocab-registry/data'))
records_directory = os.path.join(data_directory, 'records')

# class MinimumVocabInfoLOV(BaseModel):
#     homepage: Optional[str] = None
#     nsp: Optional[str] = None
#     prefix: Optional[str] = None
#     uri: Optional[str] = None

# @celery.task(name='rdf.bartoc')

def get_cmdi_namespaces(records_path):
    # CMDI records side
    '''creates a dictionary of ids and namespaces for CMDI records'''
    # store keys and values in lists
    cmdi_file_ids = []
    cmdi_record_ids = []
    cmdi_record_ns = []
    # get cmdi file Id (iterate through all records)
    for (dirpath, dirnames, filenames) in os.walk(records_path):
        if dirpath == records_path:
            for f in filenames:
                file_id = os.path.splitext(f)[0]
                cmdi_file_ids.append(file_id)
    # for each cmdi file Id get record Id and namespace uri
    for file_id in cmdi_file_ids:
        record = get_record(file_id)
        cmdi_record_id = record.id
        if cmdi_record_id is not None:
            cmdi_record_ids.append(cmdi_record_id)
            # go to the last version and locate the summary
            summary = record.versions[-1].summary
            if summary is not None:
                # get the namespace URI
                namespace = summary.namespace.uri
                if namespace is not None:
                    cmdi_record_ns.append(namespace)
    # make a dictionary with cmdi record id and namespace
    ns_dictionary = {key:value for key,value in zip(cmdi_record_ids,cmdi_record_ns)}
    return ns_dictionary

# BARTOC records side
def suggest_bartoc_uri(cmdi_record_id):
    '''Gets Bartoc URIs from suggested vocabularies that could match the cmdi vocab
    (bartocURI is their unique URI Id, in Bartoc)'''
    bartoc_suggested_uris = []
    response = session.get(bartoc_api_suggest, params={'search': cmdi_record_id})
    if response.status_code == requests.codes.ok:
        data = response.json()
        uri_candidates = data[3]
        bartoc_suggested_uris.extend(uri_candidates)
    return bartoc_suggested_uris

def get_bartoc_url(uri):
    '''Gets the Bartoc URL using the Bartoc URI (Bartoc URL is the namespace we are after)'''
    response = session.get(bartoc_api_voc, params={'uri': uri})
    if response.status_code == requests.codes.ok:
        data_vocab = response.json()
        try:
            vocab_url = data_vocab[0]['url']
            # print(vocab_url)
        except (IndexError, KeyError) as e:
            vocab_url = f"Error {type(e).__name__} - {str(e)}"
    else:
        vocab_url = f"Error: HTTP status {response.status_code}"
    return vocab_url

# MATCHING
bartoc_suggested_uris = []
bartoc_urls = []
dictionary_test = get_cmdi_namespaces(records_directory)
for key in dictionary_test.keys():
    bartoc_uri = suggest_bartoc_uri(key)
    bartoc_suggested_uris.extend(bartoc_uri)
# print(bartoc_suggested_uris)
    for uri in bartoc_suggested_uris:
        bartoc_url = get_bartoc_url(uri)
        if dictionary_test[key] == bartoc_url:
            print(f'URI = {uri}\n Publisher = https://bartoc.org \n Name = BARTOC')

