import os
import tasks

if __name__ == '__main__':
    # tasks.cache_files.delay('lexvo')

    path = '/Users/kerim/git/vocab-registry-data/records'
    for (dirpath, dirnames, filenames) in os.walk(path):
        if dirpath == path:
            for f in filenames:
                id = os.path.splitext(f)[0]
                tasks.run_for.delay(id)
