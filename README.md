# FAIR vocabularies workers

This repository contains code and documentation of one of the components of the FAIR vocabulary registry (i.e., the
vocabulary workers). General information about the CLARIAH/SSHOC.nl FAIR vocabularies registry project can be found
here: https://github.com/CLARIAH/vocab-registry.

The FAIR vocabulary workers are implemented in Python 3 using
the [Celery distributed task queue](https://docs.celeryq.dev). The workers run on every update of a vocabulary in the
FAIR vocabulary registry.

## Working with the code

The code is organized in a number of tasks that can be found in the `vocab.tasks` module. Each task can be run
individually for debugging purposes giving it the path to the CMDI vocabulary record as an argument. The file should
follow the `record-<nr>.xml` naming convention. You can also run a pipeline of tasks by running the `vocab.tasks.run`
module with a path to the CMDI vocabulary record or a folder containing the CMDI records as an argument. To start a
Celery worker, run the `vocab.tasks.app` module with the `worker` argument. If you want to
run [Flower](https://flower.readthedocs.io) to monitor the Celery workflows, then give the `flower` argument.

Configuration is done using environment variables. Also `.env` files are picked up. The following environment variables
are used:

| Environment variable | Description                                              | Default value            |
|----------------------|----------------------------------------------------------|--------------------------|
| `REDIS_URI`          | URI of the Redis server                                  | `redis://localhost/0`    |
| `LOG_LEVEL`          | Log level                                                | `INFO`                   |
| `CONCURRENCY`        | Number of concurrent tasks                               | `10`                     |
| `VOCAB_REGISTRY_URL` | URL of the FAIR vocabulary registry                      | `https://localhost:5000` |
| `VOCAB_STATIC_URL`   | URL for serving static files                             | `https://localhost:5000` |
| `SPARQL_URL`         | URL of the SPARQL endpoint                               | `https://localhost:5000` |
| `SPARQL_UPDATE_URL`  | URL of the SPARQL update endpoint                        | `https://localhost:5000` |
| `ROOT_PATH`          | Root path of the directory containing the static files   | `./data`                 |
| `JSONLD_REL_PATH`    | Relative path to the folder with the JSON-LD files       | `jsonld`                 |
| `DOCS_REL_PATH`      | Relative path to the folder with the documentation files | `docs`                   |
| `CACHE_REL_PATH`     | Relative path to the folder with the cache               | `cache`                  |

## Tasks

The following tasks are implemented:

### Cache task: `vocab.tasks.cache`

This task caches versions of the vocabulary mentioned in a vocabulary record. It looks for URLs in the version records
with a location type attribute `dump` and downloads the content of the URL. If the file is compressed using `bzip2` or
`gzip`, then it is decompressed first. If the file is a `zip` file, then it will take out the file using the path
mentioned in the URL after the `#` character. It will look for the filename in the header to determine the file
extension. If there is no filename in the header, it will look for the content type and use it to determine the correct
file extension. The content is then compressed using `gzip` and stored in the configured `CACHE_REL_PATH` location. The
URL for the cached file using the `VOCAB_STATIC_URL` is then written to the record as a location with a `dump` attribute
and a `cache` recipe attribute. If there was already a cached file for the version, then the task will not download the
file again.

### Documentation task: `vocab.tasks.documentation`

This task generates the documentation for the vocabulary mentioned in a vocabulary record if it is of an RDF type. It
uses the cache and [pyLODE](https://github.com/RDFLib/pyLODE) to generate the documentation. The documentation is then
compressed using `gzip` and stored in the configured `DOCS_REL_PATH` location. The URL for the documentation using the
`VOCAB_STATIC_URL` is then written to the record as a location with a `homepage` attribute and a `doc` recipe attribute.
If there was already documentation generated for the version, then the task will not generate documentation again.

### SPARQL task: `vocab.tasks.sparql`

This task updates the SPARQL endpoint with the vocabulary mentioned in a vocabulary record using the `SPARQL_UPDATE_URL`
if it is of an RDF type. It uses the cache to insert the RDF data in its own graph in the SPARQL store. If there was
already data found for a version in the SPARQL store using the `SPARQL_URL`, then the task will not update the SPARQL
store again.

### Summarizer task: `vocab.tasks.summarizer`

This task generates a summary of the vocabulary mentioned in a vocabulary record if it is of an RDF type. It uses the
cache to read the RDF data into a memory RDF model and then generates a summary of each version of the vocabulary. The
summaries are then written back into the CMDI record.

### LOV task: `vocab.tasks.lov`

This task queries the [Linked Open Vocabularies (LOV)](https://lov.linkeddata.es/dataset/lov/vocabs) with the vocabulary
mentioned in a vocabulary record if it is of an RDF type. If the vocabulary was found in the LOV dataset, then the task
will write the LOV URI as a publisher to the record. Furthermore, it will update the namespace of the vocabulary in the
record.

### Bartoc task: `vocab.tasks.bartoc`

This task queries [Bartoc](https://bartoc.org/) with the vocabulary mentioned in a vocabulary record if it is of an
RDF type. If the vocabulary was found in Bartoc, then the task will write the Bartoc URI as a publisher to the record.

### JSON-LD task: `vocab.tasks.jsonld`

This task will generate an RDF version of a vocabulary record. The RDF version is serialized to the JSON-LD format and
is compressed using `gzip` and stored in the configured `JSONLD_REL_PATH` location. The RDF data is also written to the
SPARQL store using the `SPARQL_UPDATE_URL`.

### Skosmos task: `vocab.tasks.skosmos`

This task will load the SKOS vocabulary mentioned in a vocabulary record into [Skosmos](https://skosmos.org/) if it is
of an `skos` type. It will use a reference to the graph of a version of the vocabulary in the SPARQL store using the
`SPARQL_URL` and update the Skosmos configuration file.
