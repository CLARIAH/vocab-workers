import os

redis_uri = os.environ.get('REDIS_URI', 'redis://localhost/0')
log_level = os.environ.get('LOG_LEVEL', 'INFO')
concurrency = os.environ.get('CONCURRENCY', 10)

vocab_registry_url = os.environ.get('VOCAB_REGISTRY_URL', 'https://localhost:5000')
vocab_static_url = os.environ.get('VOCAB_STATIC_URL', 'https://localhost:5000')
sparql_url = os.environ.get('SPARQL_URL', 'https://localhost:5000')
sparql_update_url = os.environ.get('SPARQL_UPDATE_URL', 'https://localhost:5000')

root_path = os.environ.get('ROOT_PATH')
records_rel_path = os.environ.get('RECORDS_REL_PATH', 'records')
jsonld_rel_path = os.environ.get('JSONLD_REL_PATH', 'jsonld')
docs_rel_path = os.environ.get('DOCS_REL_PATH', 'docs')
cache_rel_path = os.environ.get('CACHE_REL_PATH', 'cache')
