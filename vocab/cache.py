import os
import re
import gzip
import logging

import requests

from cmdi import get_record, write_location
from config import cache_path, vocab_cache_url
from util import content_type_extensions

log = logging.getLogger(__name__)


def get_relative_path_for_file(id, version, extension):
    return id + '/' + version + extension


def cache_files(id):
    def cache_for_file(url, version):
        response = requests.get(url, allow_redirects=True)
        if response.ok:
            file_name, file_extension = os.path.splitext(url)
            content = response.content
            if file_extension == '.gz':
                file_name, file_extension = os.path.splitext(file_name)
            else:
                content = gzip.compress(content)

            content_type = response.headers.get('content-type')
            if content_type:
                content_type = content_type.split(';')[0]

            if not file_extension and content_type:
                if content_type in content_type_extensions:
                    file_extension = content_type_extensions[content_type]
                else:
                    log.warning(f"No file extension, but we have a content type for {id}: {content_type}!")

            if file_extension:
                cached_file_name = cache_path + get_relative_path_for_file(id, version, file_extension) + '.gz'
                os.makedirs(os.path.dirname(cached_file_name), exist_ok=True)
                open(cached_file_name, 'wb').write(content)

                uri = f'{vocab_cache_url}/{get_relative_path_for_file(id, version, file_extension)}'
                write_location(id, version, uri, 'endpoint', 'cache')

                log.info(f"Cache created for {id}: {location['location']}!")
            else:
                log.error(f"No file extension found for {id}: {location['location']}!")
        else:
            log.error(f"Failed to create cache for {id}: {location['location']}!")

    record = get_record(id)
    if record and 'versions' in record and record['versions']:
        for version in record['versions']:
            for location in version['locations']:
                if location['type'] == 'endpoint' and location['recipe'] is None:
                    found_cache = False
                    folder = f'{cache_path}/{id}'

                    if os.path.exists(folder):
                        for filename in os.listdir(folder):
                            if re.search(version['version'], filename):
                                found_cache = True

                    if not found_cache:
                        log.info(f"No cache found for {id}: {location['location']}, creating!")
                        cache_for_file(location['location'], version['version'])
    else:
        log.info(f'No record or versions found for {id}!')
