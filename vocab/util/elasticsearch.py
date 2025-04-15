import urllib3

from elasticsearch import Elasticsearch

from vocab.config import elasticsearch_uri, elasticsearch_user, elasticsearch_password

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

es = Elasticsearch(
    elasticsearch_uri,
    basic_auth=(elasticsearch_user, elasticsearch_password) if elasticsearch_user and elasticsearch_password else None,
    verify_certs=False,
    retry_on_timeout=True
)
