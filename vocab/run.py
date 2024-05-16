import os

from vocab.tasks import cache, sparql, documentation, lov, summarizer, skosmos

if __name__ == '__main__':
    # Run one task for one specific vocabulary
    lov.lov.delay('sdo')

    # path = '/Users/kerim/git/vocab-registry-data/records'
    # for (dirpath, dirnames, filenames) in os.walk(path):
    #     if dirpath == path:
    #         for f in filenames:
    #             id = os.path.splitext(f)[0]
    #             summarizer.summarizer.delay(id)
