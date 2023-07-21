import os
import tasks

if __name__ == '__main__':
    # tasks.create_documentation('yalc-gold')

    path = '/Users/kerim/git/vocab-registry/data/records'
    for (dirpath, dirnames, filenames) in os.walk(path):
        if dirpath == path:
            for f in filenames:
                id = os.path.splitext(f)[0]
                tasks.create_documentation.delay(id)
