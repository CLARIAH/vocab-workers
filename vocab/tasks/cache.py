import bz2
import os
import re
import gzip
import logging
import requests

from io import BytesIO
from zipfile import ZipFile

from vocab.app import celery
from vocab.cmdi import with_version, write_location
from vocab.config import cache_path, vocab_static_url
from vocab.util.rdf import content_type_extensions
from vocab.util.fs import get_cached_version

log = logging.getLogger(__name__)


def get_relative_path_for_file(id: str, version: str, extension: str) -> str:
    return os.path.join(id, version + extension)


@celery.task(name='cache')
def cache_files(id: str) -> None:
    for record, version in with_version(id):
        for location in version.locations:
            if location.type == 'dump':
                cached_path = get_cached_version(id, version.version)
                if cached_path is None:
                    try:
                        log.info(f"No cache found for {id}: {location.location}, creating!")
                        cache_for_file(location.location, id, version.version)
                    except Exception as e:
                        log.error(f'Failed to cache for {id}: {location.location}: {e}')
                else:
                    log.info(f"Write cache location fo {id} and version {version.version}")
                    write_cache_location(id, version.version, cached_path)


def cache_for_file(url: str, id: str, version: str) -> None:
    response = requests.get(url, allow_redirects=True)
    if response.ok:
        url_hash = ''
        if '#' in url:
            url, url_hash = url[0:url.index('#')], url[url.index('#') + 1:]

        content_disposition = response.headers.get('content-disposition', '')
        file_name_regex = re.findall('filename=(.+)', content_disposition)
        file_name = file_name_regex[0] if file_name_regex else url

        file_name, file_extension = os.path.splitext(file_name)
        content = response.content

        if file_extension == '.zip' and url_hash:
            file_name, file_extension = os.path.splitext(url_hash)
            with ZipFile(BytesIO(content)) as zip, zip.open(url_hash) as file:
                content = file.read()

        if file_extension == '.bz2':
            file_name, file_extension = os.path.splitext(file_name)
            content = bz2.decompress(content)

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

            uri = f'{vocab_static_url}/cache/{get_relative_path_for_file(id, version, file_extension)}'
            write_location(id, version, uri, 'dump', 'cache')

            log.info(f"Cache created for {id} and version {version}!")
        else:
            log.error(f"No file extension found for {id} and version {version}!")
    else:
        log.error(f"Failed to create cache for {id} and version {version}!")


def write_cache_location(id: str, version: str, cached_path: str) -> None:
    file_name, file_extension = os.path.splitext(cached_path[:-3])
    uri = f'{vocab_static_url}/cache/{get_relative_path_for_file(id, version, file_extension)}'
    write_location(id, version, uri, 'dump', 'cache')
