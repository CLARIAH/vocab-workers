import os
import sys

from vocab.pipeline import run_pipeline_with_file

if __name__ == '__main__':
    filename = sys.argv[1]

    if os.path.isfile(filename):
        run_pipeline_with_file(filename)
    else:
        for (dirpath, dirnames, filenames) in os.walk(filename):
            if dirpath == filename:
                for f in filenames:
                    run_pipeline_with_file(f)
