import requests
from requests.adapters import HTTPAdapter, Retry


class Worker:
    def __init__(self):
        self._session = requests.Session()
        self._session.mount('https://', HTTPAdapter(max_retries=Retry(total=10, backoff_factor=1)))

    def run(self, id):
        pass
