import os

redis_uri = os.environ.get('REDIS_URI', 'redis://localhost/0')
log_level = os.environ.get('LOG_LEVEL', 'INFO')
concurrency = os.environ.get('CONCURRENCY', 10)

vocab_registry_url = os.environ.get('VOCAB_REGISTRY_URL', 'https://localhost:5000')
vocab_cache_url = os.environ.get('VOCAB_CACHE_URL', 'https://localhost:5000')
summarizer_url = os.environ.get('SUMMARIZER_URL', 'https://api.zandbak.dans.knaw.nl/summarizer')

records_path = os.environ.get('RECORDS_PATH')
docs_path = os.environ.get('DOCS_PATH')
cache_path = os.environ.get('CACHE_PATH')
