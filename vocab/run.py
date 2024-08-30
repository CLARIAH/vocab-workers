import os

from vocab.tasks import cache, sparql, documentation, jsonld, lov, summarizer, skosmos

if __name__ == '__main__':
    # Run one task for one specific vocabulary
    # jsonld.create_jsonld.delay('pico')

    path = '/Users/kerim/git/vocab-registry-data/records'
    for (dirpath, dirnames, filenames) in os.walk(path):
        if dirpath == path:
            for f in filenames:
                id = os.path.splitext(f)[0]
                jsonld.create_jsonld.delay(id)
