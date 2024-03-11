import os

from vocab.tasks import cache, sparql, documentation, skosmos

if __name__ == '__main__':
    skosmos.add_to_skosmos_config.delay('umbel')

    # path = '/Users/kerim/git/vocab-registry-data/records'
    # for (dirpath, dirnames, filenames) in os.walk(path):
    #     if dirpath == path:
    #         for f in filenames:
    #             id = os.path.splitext(f)[0]
    #             documentation.create_documentation.delay(id)
