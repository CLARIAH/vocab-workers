from requests import Session
from requests.adapters import HTTPAdapter, Retry

session = Session()
session.mount('https://', HTTPAdapter(max_retries=Retry(total=10, backoff_factor=1)))
